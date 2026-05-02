import argparse
import json
import logging
import re
from typing import Any, List, Dict, Optional, Tuple, Set
from urllib.parse import urlparse

import boto3
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, ArrayType, MapType, StringType, DataType
)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IcebergIngestion:
    """
    Classe para ingestão resiliente de JSON complexo/aninhado na camada RAW Iceberg.

    Premissas:
      - Schema 100% dinâmico — inferido do JSON, sem hardcode de campos
      - Resolve automaticamente o limite do Glue (131.072 chars por tipo de coluna)
      - Structs com chaves dinâmicas (ex: CVE IDs, hashes) → MapType automaticamente
      - Nomes de campos com caracteres especiais → sanitizados
      - Metadados de ingestão (_ingested_at, _source_file) adicionados automaticamente
    """

    # Limite do Glue Data Catalog para o tipo serializado de uma coluna (128 KB)
    GLUE_TYPE_LIMIT = 131_072

    # Structs com mais campos do que este número → tratados como chaves dinâmicas
    DYNAMIC_KEY_THRESHOLD = 20

    # Regex: padrões de nomes que indicam chaves dinâmicas (não campos fixos de schema)
    DYNAMIC_KEY_PATTERN = re.compile(
        r'^('
        r'CVE-\d{4}-\d+'       r'|'   # CVE-2024-12345
        r'[0-9a-f]{8,}'        r'|'   # hashes hex
        r'\d{4}-\d{2}-\d{2}.*' r'|'   # datas como chave
        r'[A-Z]{2,3}-\d+'      r'|'   # JIRA-style (ABC-123)
        r'\d+\.\d+\.\d+.*'            # versões semânticas
        r')',
        re.IGNORECASE
    )

    # =========================================================================
    # BUSCA DE ARQUIVOS NO S3
    # =========================================================================

    @classmethod
    def find_files_by_date_pattern(
        cls,
        s3_path: str,
        execution_date: str,
        file_pattern: Optional[str] = None
    ) -> List[str]:
        """
        Busca arquivos no S3 que correspondem ao padrão de data informado.

        Args:
            s3_path: Caminho base no S3 (ex: s3://bucket/prefix/)
            execution_date: Data de execução (formato YYYY-MM-DD)
            file_pattern: Padrão regex com placeholder {date} (opcional)

        Returns:
            Lista de caminhos S3 completos dos arquivos encontrados
        """
        parsed = urlparse(s3_path)
        if not parsed.netloc:
            raise ValueError(f"Caminho S3 inválido: {s3_path}")

        bucket = parsed.netloc
        prefix = parsed.path.lstrip('/')

        s3_client = boto3.client('s3')
        matching_files = []

        if file_pattern:
            pattern = file_pattern.replace("{date}", execution_date)
        else:
            pattern = execution_date

        logger.info(f"Buscando arquivos no bucket '{bucket}' com padrão '{pattern}'")

        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    file_key = obj['Key']
                    file_name = file_key.split('/')[-1]

                    if re.search(pattern, file_name):
                        full_path = f"s3://{bucket}/{file_key}"
                        matching_files.append(full_path)
                        logger.info(
                            f"Arquivo encontrado: {file_name} "
                            f"({obj.get('Size', 0) / 1024 / 1024:.2f} MB)"
                        )

            logger.info(f"Total: {len(matching_files)} arquivo(s) encontrado(s)")

        except Exception as e:
            logger.error(f"Erro ao listar arquivos: {e}")
            raise

        return matching_files

    # =========================================================================
    # REESCRITA DINÂMICA DO SCHEMA
    # =========================================================================

    @classmethod
    def _is_dynamic_struct(cls, struct: StructType) -> bool:
        """
        Detecta se um StructType tem chaves dinâmicas (não representa campos fixos).
        Critérios:
          1. Mais campos do que o threshold configurado
          2. Maioria dos nomes bate em padrão de chaves dinâmicas (CVE IDs, hashes, etc.)
        """
        n = len(struct.fields)
        if n > cls.DYNAMIC_KEY_THRESHOLD:
            return True
        if n == 0:
            return False
        dynamic_count = sum(1 for f in struct.fields if cls.DYNAMIC_KEY_PATTERN.match(f.name))
        return dynamic_count / n > 0.5

    @classmethod
    def _infer_map_value_type(cls, struct: StructType) -> DataType:
        """
        Dado um struct de chaves dinâmicas, infere o tipo do value do MapType resultante.
        - Se todos os campos têm o mesmo subtipo → usa esse subtipo (reescrito recursivamente)
        - Caso contrário → StringType (mais seguro para raw)
        """
        if not struct.fields:
            return StringType()

        first_type = struct.fields[0].dataType
        all_same = all(
            str(f.dataType) == str(first_type) for f in struct.fields
        )
        if all_same:
            return cls.rewrite_type(first_type)

        return StringType()

    @classmethod
    def rewrite_type(cls, dtype: DataType, path: str = "") -> DataType:
        """
        Reescreve recursivamente um DataType:
          - StructType com chaves dinâmicas    → MapType<string, value_type>
          - StructType com tipo acima do limite → StringType (serializado como JSON)
          - StructType normal                  → reescreve cada campo recursivamente
          - ArrayType                          → reescreve elementType
          - MapType                            → reescreve valueType
          - Primitivos                         → sem alteração
        """
        if isinstance(dtype, StructType):

            # Tipo inteiro acima do limite do Glue → serializa como string JSON
            if len(str(dtype)) > cls.GLUE_TYPE_LIMIT:
                logger.warning(
                    f"[{path}] tipo excede limite do Glue "
                    f"({len(str(dtype)):,} chars) → StringType (parse na trusted)"
                )
                return StringType()

            # Struct com chaves dinâmicas → MapType
            if cls._is_dynamic_struct(dtype):
                value_type = cls._infer_map_value_type(dtype)
                logger.info(
                    f"[{path}] struct com {len(dtype.fields)} campos dinâmicos "
                    f"→ MapType<string, {value_type}>"
                )
                return MapType(StringType(), value_type, True)

            # Struct normal → processa cada campo recursivamente
            new_fields = []
            for f in dtype.fields:
                field_path = f"{path}.{f.name}" if path else f.name

                # Sanitiza nome do campo (remove chars especiais que quebram Glue/Iceberg)
                clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', f.name)
                if clean_name != f.name:
                    logger.info(f"[{field_path}] renomeado: '{f.name}' → '{clean_name}'")

                new_type = cls.rewrite_type(f.dataType, field_path)
                new_fields.append(StructField(clean_name, new_type, True))

            return StructType(new_fields)

        elif isinstance(dtype, ArrayType):
            return ArrayType(
                cls.rewrite_type(dtype.elementType, f"{path}[]"),
                dtype.containsNull
            )

        elif isinstance(dtype, MapType):
            return MapType(
                dtype.keyType,
                cls.rewrite_type(dtype.valueType, f"{path}[v]"),
                dtype.valueContainsNull
            )

        else:
            return dtype

    @classmethod
    def rewrite_schema(cls, schema: StructType) -> StructType:
        """Ponto de entrada: reescreve o schema raiz completo."""
        new_fields = []
        for f in schema.fields:
            clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', f.name)
            new_type = cls.rewrite_type(f.dataType, f.name)
            new_fields.append(StructField(clean_name, new_type, True))
        return StructType(new_fields)

    # =========================================================================
    # APLICAR CORREÇÕES NO DATAFRAME
    # =========================================================================

    @staticmethod
    def apply_fixes(df: DataFrame, original_schema: StructType, new_schema: StructType) -> DataFrame:
        """
        Aplica transformações nas colunas onde o tipo mudou.
        Verifica o tipo real no DF para evitar to_json em strings.
        """
        current_df_types = {field.name: field.dataType for field in df.schema.fields}

        for orig_f, new_f in zip(original_schema.fields, new_schema.fields):
            orig_name = orig_f.name
            new_name = new_f.name

            actual_type_in_df = current_df_types.get(orig_name)

            if (
                isinstance(new_f.dataType, StringType)
                and not isinstance(orig_f.dataType, StringType)
                and not isinstance(actual_type_in_df, StringType)
            ):
                logger.info(f"Serializando '{orig_name}' → to_json()")
                df = df.withColumn(orig_name, F.to_json(F.col(f"`{orig_name}`")))

            if orig_name != new_name:
                df = df.withColumnRenamed(orig_name, new_name)

        return df

    # =========================================================================
    # VALIDAÇÃO PRÉ-COMMIT
    # =========================================================================

    @classmethod
    def validate_glue_limits(cls, schema: StructType, table_name: str):
        """Garante que nenhum campo ainda excede o limite do Glue."""
        violations = [
            f"  '{f.name}': {len(str(f.dataType)):,} chars"
            for f in schema.fields
            if len(str(f.dataType)) > cls.GLUE_TYPE_LIMIT
        ]
        if violations:
            raise ValueError(
                f"Schema de '{table_name}' ainda tem campos acima do limite do Glue "
                f"({cls.GLUE_TYPE_LIMIT:,} chars):\n" + "\n".join(violations)
            )
        logger.info("Schema validado — todos os campos dentro do limite do Glue")

    # =========================================================================
    # LEITURA RESILIENTE
    # =========================================================================

    @classmethod
    def _merge_with_table_schema(cls, rewritten_schema: StructType, table_schema: StructType) -> StructType:
        """
        Mescla o schema reescrito com o schema da tabela existente.
        Para colunas que já existem na tabela, usa o tipo da tabela (evita MAP vs STRUCT).
        Para colunas novas, mantém o tipo inferido/reescrito.
        """
        table_fields_map = {f.name.lower(): f for f in table_schema.fields}
        merged_fields = []

        for field in rewritten_schema.fields:
            table_field = table_fields_map.get(field.name.lower())
            if table_field:
                # Coluna já existe na tabela → usa o tipo da tabela
                merged_fields.append(StructField(table_field.name, table_field.dataType, True))
                logger.info(
                    f"[merge] '{field.name}' → usando tipo da tabela: "
                    f"{table_field.dataType.simpleString()}"
                )
            else:
                # Coluna nova → usa o tipo reescrito
                merged_fields.append(field)

        return StructType(merged_fields)

    @classmethod
    def read_json(cls, spark: SparkSession, input_path, table_schema: Optional[StructType] = None) -> Tuple[DataFrame, StructType]:
        """
        Leitura resiliente em 3 passos:
          1. Inferência do schema (amostra)
          2. Reescrita do schema (resolve problemas do Glue)
          3. Merge com schema da tabela (se existente) para evitar conflitos de tipo
          4. Releitura com schema corrigido

        Args:
            spark: SparkSession
            input_path: Caminho S3 (str) ou lista de caminhos S3 (List[str])
            table_schema: Schema da tabela existente (opcional). Se fornecido,
                          colunas conhecidas usam o tipo da tabela.
        """
        logger.info(f"Lendo JSON de: {input_path}")

        # Passo 1: inferência
        logger.info("Inferindo schema...")
        df_infer = spark.read \
            .option("inferSchema", "true") \
            .option("multiLine", "true") \
            .option("mode", "PERMISSIVE") \
            .json(input_path)

        original_schema = df_infer.schema
        logger.info(
            f"Schema inferido: {len(original_schema.fields)} campos raiz | "
            f"tamanho total: {sum(len(str(f.dataType)) for f in original_schema.fields):,} chars"
        )

        # Passo 2: reescrita
        logger.info("Reescrevendo schema (resolução de tipos dinâmicos e limites do Glue)...")
        new_schema = cls.rewrite_schema(original_schema)

        # Passo 3: merge com schema da tabela existente (se houver)
        if table_schema:
            logger.info("Mesclando schema com tabela existente (prioridade para tipos da tabela)...")
            new_schema = cls._merge_with_table_schema(new_schema, table_schema)

        # Passo 4: releitura com schema corrigido
        logger.info("Relendo com schema corrigido...")
        df = spark.read \
            .schema(new_schema) \
            .option("multiLine", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .json(input_path)

        # Aplica fixes residuais (to_json em campos que ficaram oversized)
        df = cls.apply_fixes(df, original_schema, new_schema)

        # Remove coluna de corrompidos se vazia
        if "_corrupt_record" in df.columns:
            corrupt = df.filter(F.col("_corrupt_record").isNotNull()).count()
            if corrupt > 0:
                logger.warning(f"Registros corrompidos: {corrupt}")
                df.filter(F.col("_corrupt_record").isNotNull()) \
                  .select("_corrupt_record").show(5, truncate=120)
            df = df.drop("_corrupt_record")

        # Metadados de ingestão
        df = df \
            .withColumn("_raw_ingested_at", F.current_timestamp()) \
            .withColumn("_source_file", F.input_file_name())

        logger.info(f"Registros lidos: {df.count():,}")
        return df, new_schema

    # =========================================================================
    # SINCRONIZAÇÃO DE SCHEMA (EVOLUÇÃO)
    # =========================================================================

    @classmethod
    def escape_field_names_in_type(cls, type_str: str) -> str:
        """Escapa nomes de campos com caracteres especiais no DDL de tipo."""
        def escape_match(m):
            prefix = m.group(1)
            field_name = m.group(2).strip()
            if re.search(r'[^a-zA-Z0-9_]', field_name):
                return f'{prefix}`{field_name}`:'
            return m.group(0)

        return re.sub(r'(struct<|,\s*)(?!`)([^:<>,`]+):', escape_match, type_str)

    @classmethod
    def sync_schema(cls, spark: SparkSession, df: DataFrame, table_identifier: str) -> None:
        """
        Adiciona ao Iceberg colunas que existem no DataFrame mas não na tabela.
        Garante schema evolution sem perda de dados.
        """
        try:
            table_schema = spark.table(table_identifier).schema
            existing_cols = {f.name.lower() for f in table_schema.fields}
            new_cols = [f for f in df.schema.fields if f.name.lower() not in existing_cols]

            if not new_cols:
                logger.info("Schema sincronizado — nenhuma coluna nova.")
                return

            logger.info(f"Adicionando {len(new_cols)} nova(s) coluna(s) em {table_identifier}:")

            for field in new_cols:
                dtype = field.dataType.simpleString()
                dtype_escaped = cls.escape_field_names_in_type(dtype)
                try:
                    logger.info(f"    + {field.name} ({dtype_escaped})")
                    spark.sql(f"ALTER TABLE {table_identifier} ADD COLUMN `{field.name}` {dtype_escaped}")
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        logger.warning(f"Erro ao adicionar {field.name}: {e}")

            spark.catalog.refreshTable(table_identifier)

        except Exception as e:
            logger.error(f"Erro na sincronização de schema: {e}")
            raise

    @classmethod
    def align_dataframe_to_table(cls, df: DataFrame, table_schema: StructType) -> DataFrame:
        """
        Alinha o DataFrame ao schema da tabela Iceberg:
          - Colunas da tabela que não existem no DF → adicionadas como null
          - Ordem das colunas → segue a ordem da tabela
          - Tipos incompatíveis (MAP vs STRUCT, etc.) → serializa como JSON string
        """
        select_exprs = []
        df_fields_map = {f.name.lower(): f for f in df.schema.fields}

        for table_field in table_schema.fields:
            df_field = df_fields_map.get(table_field.name.lower())

            if df_field:
                col_expr = F.col(f"`{df_field.name}`")

                # Se a tabela espera String mas o DF tem Struct/Array/Map → serializa como JSON
                if isinstance(table_field.dataType, StringType) and isinstance(
                    df_field.dataType, (StructType, ArrayType, MapType)
                ):
                    select_exprs.append(F.to_json(col_expr).alias(table_field.name))

                # Tipos fundamentalmente incompatíveis (ex: MAP vs STRUCT) → to_json + null
                elif (
                    isinstance(df_field.dataType, MapType)
                    and isinstance(table_field.dataType, StructType)
                ):
                    logger.warning(
                        f"Tipo incompatível para '{table_field.name}': "
                        f"DF={df_field.dataType.simpleString()} vs "
                        f"Tabela={table_field.dataType.simpleString()}. "
                        f"Usando null (dados serão preservados em _source_file para reprocessamento)."
                    )
                    select_exprs.append(F.lit(None).cast(table_field.dataType).alias(table_field.name))

                elif (
                    isinstance(df_field.dataType, StructType)
                    and isinstance(table_field.dataType, MapType)
                ):
                    logger.warning(
                        f"Tipo incompatível para '{table_field.name}': "
                        f"DF=STRUCT vs Tabela=MAP. Serializando como JSON."
                    )
                    select_exprs.append(F.to_json(col_expr).alias(table_field.name))

                else:
                    # Cast normal (compatível)
                    select_exprs.append(col_expr.cast(table_field.dataType).alias(table_field.name))
            else:
                # Coluna existe na tabela mas não no DF → null
                select_exprs.append(F.lit(None).cast(table_field.dataType).alias(table_field.name))

        return df.select(*select_exprs)

    # =========================================================================
    # ESCRITA NO ICEBERG
    # =========================================================================

    @staticmethod
    def create_if_not_exists(spark: SparkSession, table_name: str, df: DataFrame, partition_by: List[str] = None):
        """Cria a tabela Iceberg se não existir usando o schema do DataFrame."""
        try:
            spark.table(table_name)
            logger.info(f"Tabela '{table_name}' já existe.")
            return False
        except Exception:
            pass

        logger.info(f"Criando tabela '{table_name}'...")

        writer = spark.createDataFrame([], df.schema) \
            .writeTo(table_name) \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .tableProperty("write.metadata.delete-after-commit.enabled", "true") \
            .tableProperty("write.metadata.previous-versions-max", "10")

        if partition_by:
            writer = writer.partitionedBy(*partition_by)
            logger.info(f"Particionamento: {partition_by}")

        writer.createOrReplace()
        logger.info(f"Tabela '{table_name}' criada.")
        return True

    @classmethod
    def write_raw(cls, spark: SparkSession, df: DataFrame, table_name: str, mode: str = "append"):
        """
        Sincroniza o schema, alinha o DataFrame e escreve na tabela Iceberg.
        Lida com schema evolution automaticamente.
        """
        cls.validate_glue_limits(df.schema, table_name)

        # Sincroniza: adiciona colunas novas do DF na tabela
        cls.sync_schema(spark, df, table_name)

        # Alinha: garante que o DF tem todas as colunas da tabela na ordem correta
        table_schema = spark.table(table_name).schema
        df_aligned = cls.align_dataframe_to_table(df, table_schema)

        logger.info(f"Escrevendo em '{table_name}' (mode={mode})...")
        writer = df_aligned.writeTo(table_name).option("check-ordering", "false")

        if mode == "overwrite":
            writer.overwritePartitions()
        else:
            writer.append()

        logger.info(f"Escrita concluída em '{table_name}'")

    # =========================================================================
    # PIPELINE PRINCIPAL
    # =========================================================================

    @classmethod
    def run_ingestion(cls, spark: SparkSession, execute_date: str, config: Dict[str, Any]):
        """
        Executa o pipeline completo de ingestão RAW Iceberg.

        Args:
            spark: SparkSession configurada
            execute_date: Data de execução (formato YYYY-MM-DD)
            config: Dicionário de configuração contendo:
                - source.input_path: caminho S3 dos JSONs de entrada
                - destination.catalog_name: nome do catálogo Iceberg
                - destination.database: nome do database
                - destination.table: nome da tabela
                - destination.s3_location (opcional): localização S3 da tabela
                - destination.mode (opcional): modo de escrita (append/overwrite)
        """
        source = config.get("source", {})
        destination = config.get("destination", {})

        input_path = source.get("input_path")
        if not input_path:
            raise ValueError("'source.input_path' é obrigatório na configuração.")

        file_pattern = source.get("file_pattern")

        catalog_name = destination.get("catalog_name", "glue_catalog")
        database = destination.get("database")
        table = destination.get("table")
        mode = destination.get("write_mode", destination.get("mode", "append"))
        partition_by = destination.get("partition_by", [])

        if not database or not table:
            raise ValueError("'destination.database' e 'destination.table' são obrigatórios.")

        table_name = f"{catalog_name}.{database}.{table}"

        logger.info("=" * 60)
        logger.info("RAW ICEBERG WRITER")
        logger.info(f"Input:        {input_path}")
        logger.info(f"Tabela:       {table_name}")
        logger.info(f"Mode:         {mode}")
        logger.info(f"Partição:     {partition_by}")
        logger.info(f"ExecuteDate:  {execute_date}")
        logger.info("=" * 60)

        # Etapa 1: Buscar arquivos no S3 pelo padrão de data
        logger.info("ETAPA 1: Buscar arquivos no S3")
        matching_files = cls.find_files_by_date_pattern(input_path, execute_date, file_pattern)

        if not matching_files:
            logger.warning(f"Nenhum arquivo encontrado para {execute_date}. Finalizando.")
            return

        # Etapa 2: Leitura e reescrita do schema
        logger.info("ETAPA 2: Leitura e reescrita do schema")

        # Se a tabela já existe, obter schema para evitar conflitos de tipo (MAP vs STRUCT)
        existing_table_schema = None
        try:
            existing_table_schema = spark.table(table_name).schema
            logger.info(f"Tabela '{table_name}' existe — schema será usado como referência.")
        except Exception:
            logger.info(f"Tabela '{table_name}' não existe ainda — schema será inferido.")

        df, schema = cls.read_json(spark, matching_files, existing_table_schema)

        # Adiciona coluna de partição execution_date com o valor da data de execução
        df = df.withColumn("execution_date", F.lit(execute_date).cast("date"))
        logger.info(f"Coluna 'execution_date' adicionada com valor: {execute_date}")

        # Etapa 3: Criar tabela e gravar dados
        logger.info("ETAPA 3: Criar/gravar na tabela Iceberg")
        cls.create_if_not_exists(spark, table_name, df, partition_by)

        # Escrita na tabela Iceberg (com sync de schema + alinhamento)
        cls.write_raw(spark, df, table_name, mode)

        logger.info("=" * 60)
        logger.info(f"RAW carregada: {table_name}")
        logger.info("=" * 60)

def create_spark_session(catalog_name: str, warehouse: str) -> SparkSession:
    """
    Cria e retorna uma SparkSession configurada para trabalhar com tabelas Iceberg usando o catálogo Glue.

    Retorna:
        SparkSession: Uma instância de SparkSession configurada para Iceberg e Glue Catalog.    
    """
    return (
        SparkSession.builder.appName("Iceberg Raw Tables")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.iceberg.planning.preserve-data-grouping", "true")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.iceberg.spark.Spark3Util$IcebergCommitProtocol",
        )
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
        .config("spark.sql.iceberg.check-ordering", "false")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .config("spark.sql.caseSensitive", "true")
        .enableHiveSupport()
        .getOrCreate()
    )

def parse_arguments() -> argparse.Namespace:
    """
    Analisa os argumentos de linha de comando para o job Spark.

    Retorna:
        argparse.Namespace: Um objeto contendo os argumentos analisados, incluindo o caminho do arquivo de configuração.
    """
    parser = argparse.ArgumentParser(description="Processa dados de pagamentos usando Spark e Iceberg.", allow_abbrev=False)
    parser.add_argument(
        "--config",
        default="{}",
        help="JSON string com configurações específicas por tabela.",
    )
    parser.add_argument(
        '--execute-date', 
        type=str,
        required=True,
        help='Data específica para processar (formato: YYYY-MM-DD)')
    args, _ = parser.parse_known_args()
    return args

def main():
    """
    Função principal do job Spark. Lê a configuração, cria a SparkSession, e processa os dados de acordo com as especificações.
    """
    args = parse_arguments()
    logger.info(f"Iniciando job de ingestão para data: {args.execute_date}")
    
    spark = None
    try:
        # Parse das configurações
        config = json.loads(args.config)
        spark = create_spark_session(config["destination"]["catalog_name"], config["destination"]["warehouse"])
        logger.info("SparkSession criada com sucesso.")
        logger.info(f"Configuração carregada: {json.dumps(config, indent=2)}")
        
        # Executa a ingestão
        IcebergIngestion.run_ingestion(spark, args.execute_date, config)
        logger.info("Job finalizado com sucesso.")
        
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao parsear JSON de configuração: {e}")
        raise
    except ValueError as e:
        logger.error(f"Erro de validação: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro durante a execução do job: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession encerrada.")


if __name__ == "__main__":
    main()