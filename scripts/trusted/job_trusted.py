import argparse
import json
import logging
import re
import traceback
from typing import Any, List, Dict, Optional, Tuple

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
    Classe responsável pela ingestão de dados na camada TRUSTED Iceberg.

    Funcionalidades:
      - Lê da tabela RAW usando partition pruning por _raw_ingested_at
      - Identifica tabelas filhas a partir de Arrays de Structs E Maps com arrays internos
      - Trata conflitos de nomes de colunas (Id vs id) renomeando maiúsculo para id_
      - Adiciona _trusted_ingested_at como partição (days transform)
      - Schema evolution automática (sync + align)
    """

    # =========================================================================
    # LEITURA DA TABELA RAW
    # =========================================================================

    @staticmethod
    def read_source_table(
        spark: SparkSession,
        source_table: str,
        timestamp_column: str,
        execute_date: str
    ) -> Optional[DataFrame]:
        """
        Lê os dados da tabela raw usando partition pruning.

        Args:
            spark: SparkSession
            source_table: Nome da tabela fonte (database.table)
            timestamp_column: Coluna de timestamp/partição para filtragem
            execute_date: Data de execução (YYYY-MM-DD)

        Returns:
            DataFrame ou None se tabela não existir
        """
        try:
            query = f"""
                SELECT *
                FROM glue_catalog.{source_table}
                WHERE DATE({timestamp_column}) = '{execute_date}'
            """
            logger.info(f"Query: {query}")
            df = spark.sql(query)

        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                logger.warning(f"Tabela {source_table} não encontrada.")
                return None
            else:
                error_msg = f"Erro ao ler tabela {source_table}: {str(e)}"
                logger.error(error_msg)
                raise ValueError(error_msg) from e

        count = df.count()
        logger.info(f"Registros lidos da tabela {source_table} para {execute_date}: {count}")
        return df

    # =========================================================================
    # TRATAMENTO DE CONFLITOS DE NOMES (Id vs id)
    # =========================================================================

    @classmethod
    def fix_duplicate_column_names(cls, df: DataFrame) -> DataFrame:
        """
        Detecta conflitos case-sensitive (ex: Id e id no mesmo nível) e
        renomeia a coluna que começa com maiúsculo, adicionando sufixo '_'.

        Ex: Id → id_, ID → id_ (mantém 'id' original intacto)
        """
        seen: Dict[str, str] = {}
        renames: Dict[str, str] = {}

        for field in df.schema.fields:
            field_lower = field.name.lower()

            if field_lower in seen:
                # Conflito detectado
                existing_name = seen[field_lower]

                if field.name[0].isupper():
                    # Coluna atual começa com maiúsculo → renomeia
                    new_name = f"{field.name.lower()}_"
                    renames[field.name] = new_name
                    logger.warning(f"Conflito case-sensitive: '{field.name}' → '{new_name}'")
                elif existing_name[0].isupper():
                    # Coluna anterior começa com maiúsculo → renomeia
                    new_name = f"{existing_name.lower()}_"
                    renames[existing_name] = new_name
                    logger.warning(f"Conflito case-sensitive: '{existing_name}' → '{new_name}'")
            else:
                seen[field_lower] = field.name

        if not renames:
            return df

        logger.info(f"Aplicando {len(renames)} renomeação(ões) de conflitos case-sensitive")
        for old_name, new_name in renames.items():
            df = df.withColumnRenamed(old_name, new_name)

        return df

    @classmethod
    def fix_nested_duplicate_names(cls, df: DataFrame) -> DataFrame:
        """
        Detecta conflitos case-sensitive dentro de structs explodidos
        (campos de tabelas filhas após expand com nested_data.*).
        """
        seen: Dict[str, str] = {}
        renames: Dict[str, str] = {}

        for field in df.schema.fields:
            field_lower = field.name.lower()

            if field_lower in seen:
                existing_name = seen[field_lower]
                if field.name[0].isupper():
                    new_name = f"{field.name.lower()}_"
                    renames[field.name] = new_name
                elif existing_name[0].isupper():
                    new_name = f"{existing_name.lower()}_"
                    renames[existing_name] = new_name
            else:
                seen[field_lower] = field.name

        if not renames:
            return df

        for old_name, new_name in renames.items():
            df = df.withColumnRenamed(old_name, new_name)
            logger.info(f"  Renomeado nested: '{old_name}' → '{new_name}'")

        return df

    # =========================================================================
    # IDENTIFICAÇÃO DE TABELAS FILHAS (Arrays + Maps com Arrays)
    # =========================================================================

    @classmethod
    def identify_nested_tables(
        cls,
        table_root: str,
        schema: StructType,
        parent_path: str = ""
    ) -> List[Tuple[str, str, Optional[StructType]]]:
        """
        Identifica campos que devem virar tabelas separadas:
          1. ArrayType com elementType StructType (array de objetos)
          2. MapType cujo valueType é STRING mas contém JSON com arrays de objetos
             (ex: RiskFindings = MAP<STRING, STRING> no raw, mas o value é JSON parseable)

        Args:
            table_root: Nome base para tabelas filhas (ex: "alerts")
            schema: Schema do DataFrame
            parent_path: Caminho pai (para recursão)

        Returns:
            Lista de tuplas: (path_completo, nome_tabela, schema_do_elemento ou None para MAP)
        """
        nested_tables = []

        for field in schema.fields:
            current_path = f"{parent_path}.{field.name}" if parent_path else field.name

            # Caso 1: Array de Structs → tabela filha direta
            if isinstance(field.dataType, ArrayType) and isinstance(
                field.dataType.elementType, StructType
            ):
                table_name_parts = current_path.lower().split('.')
                table_name = f"{table_root}_" + "_".join(table_name_parts)

                nested_tables.append((
                    current_path,
                    table_name,
                    field.dataType.elementType
                ))
                logger.info(f"Detectado Array<Struct>: {current_path} → {table_name}")

            # Caso 2: MapType → pode conter arrays de objetos no value (JSON parseável)
            elif isinstance(field.dataType, MapType):
                table_name_parts = current_path.lower().split('.')
                table_name = f"{table_root}_" + "_".join(table_name_parts)

                nested_tables.append((
                    current_path,
                    table_name,
                    None  # Schema será inferido em runtime ao parsear o JSON do value
                ))
                logger.info(f"Detectado MapType: {current_path} → {table_name} (parse em runtime)")

            # Recursão em Structs
            elif isinstance(field.dataType, StructType):
                nested = cls.identify_nested_tables(table_root, field.dataType, current_path)
                nested_tables.extend(nested)

        return nested_tables

    # =========================================================================
    # EXTRAÇÃO DE DADOS ANINHADOS
    # =========================================================================

    @classmethod
    def extract_nested_from_array(
        cls,
        df: DataFrame,
        nested_path: str,
        root_id_col: str,
        execution_date: str
    ) -> Optional[DataFrame]:
        """
        Extrai dados de um campo Array<Struct> e adiciona chaves de relacionamento.

        Args:
            df: DataFrame principal
            nested_path: Caminho do campo (ex: "Inventory.top_cves")
            root_id_col: Coluna ID raiz (ex: "AlertId")
            execution_date: Data de execução
        """
        try:
            path_parts = nested_path.split('.')

            if len(path_parts) > 1:
                parent_struct = path_parts[0]
                array_field = path_parts[-1]

                # Verifica se o struct pai tem campo 'id'
                parent_fields = [
                    f for f in df.schema.fields if f.name == parent_struct
                ]
                if not parent_fields:
                    logger.warning(f"Campo '{parent_struct}' não encontrado no schema")
                    return None

                parent_type = parent_fields[0].dataType

                has_parent_id = False
                parent_id_field = None

                if isinstance(parent_type, StructType):
                    for candidate in ['id', 'Id', f'{parent_struct.lower()}_id']:
                        if candidate in [f.name for f in parent_type.fields]:
                            parent_id_field = candidate
                            has_parent_id = True
                            break

                if has_parent_id:
                    parent_id_col_name = f"{parent_struct.lower()}_id_ref"
                    exploded_df = df.select(
                        F.col(f"`{root_id_col}`").alias(f"{root_id_col.lower()}"),
                        F.col(f"`{parent_struct}`.`{parent_id_field}`").alias(parent_id_col_name),
                        F.explode_outer(F.col(f"`{parent_struct}`.`{array_field}`")).alias("nested_data")
                    )
                else:
                    exploded_df = df.select(
                        F.col(f"`{root_id_col}`").alias(f"{root_id_col.lower()}"),
                        F.explode_outer(F.col(f"`{parent_struct}`.`{array_field}`")).alias("nested_data")
                    )
            else:
                array_field = path_parts[0]
                exploded_df = df.select(
                    F.col(f"`{root_id_col}`").alias(f"{root_id_col.lower()}"),
                    F.explode_outer(F.col(f"`{array_field}`")).alias("nested_data")
                )

            # Filtra nulos do explode_outer
            exploded_df = exploded_df.filter(F.col("nested_data").isNotNull())

            if exploded_df.count() == 0:
                return None

            # Expande struct
            select_cols = [c for c in exploded_df.columns if c != "nested_data"]
            nested_df = exploded_df.select(
                *[F.col(c) for c in select_cols],
                "nested_data.*"
            )

            # Trata conflitos Id vs id no resultado expandido
            nested_df = cls.fix_nested_duplicate_names(nested_df)

            # Adiciona metadados
            nested_df = nested_df.withColumn(
                "execution_date", F.lit(execution_date).cast("date")
            ).withColumn(
                "_trusted_ingested_at", F.current_timestamp()
            )

            return nested_df

        except Exception as e:
            logger.warning(f"Erro ao extrair Array<Struct> {nested_path}: {e}")
            logger.debug(traceback.format_exc())
            return None

    @classmethod
    def extract_nested_from_map(
        cls,
        spark: SparkSession,
        df: DataFrame,
        map_col_name: str,
        root_id_col: str,
        execution_date: str
    ) -> Optional[List[Tuple[str, DataFrame]]]:
        """
        Extrai dados de um campo MAP<STRING, STRING> cujo value é JSON parseável.
        Analisa cada chave do MAP separadamente:
          - Chaves cujo value é um JSON array de objetos → viram sub-tabelas individuais
            com sufixo baseado no nome da chave (ex: AclGrants → _aclgrants)
          - Chaves com valores escalares → agrupadas em tabela flat (key/value)

        Args:
            spark: SparkSession
            df: DataFrame principal
            map_col_name: Nome da coluna MAP
            root_id_col: Coluna ID raiz
            execution_date: Data de execução

        Returns:
            Lista de tuplas (chave_original, sufixo_tabela, DataFrame) ou None.
            Ex: ("AclGrants", "_aclgrants", df) → tabela alerts_riskfindings_aclgrants
        """
        try:
            path_parts = map_col_name.split('.')

            if len(path_parts) > 1:
                parent_struct = path_parts[0]
                map_field = path_parts[-1]
                col_ref = F.col(f"`{parent_struct}`.`{map_field}`")
            else:
                map_field = path_parts[0]
                col_ref = F.col(f"`{map_field}`")

            # Extrai o id do MAP como FK para as tabelas filhas (ex: riskfindings_id)
            parent_id_col = f"{map_field.lower()}_id"
            parent_id_expr = F.coalesce(col_ref["id"], col_ref["Id"])

            # Explode o map em key/value
            exploded_df = df.select(
                F.col(f"`{root_id_col}`").alias(f"{root_id_col.lower()}"),
                parent_id_expr.alias(parent_id_col),
                F.explode_outer(col_ref).alias("_map_key", "_map_value")
            ).filter(F.col("_map_value").isNotNull())

            if exploded_df.count() == 0:
                logger.info(f"MAP '{map_col_name}' vazio ou nulo para todos registros")
                return None

            # Identifica chaves cujo valor é JSON array (começa com '[')
            array_keys_df = exploded_df.filter(
                F.trim(F.col("_map_value")).startswith("[")
            ).select("_map_key").distinct()

            array_keys = [row["_map_key"] for row in array_keys_df.collect()]

            results: List[Tuple[str, DataFrame]] = []
            seen_suffixes: set = set()

            # Para cada chave com valor JSON array, cria uma sub-tabela
            for key in array_keys:
                suffix = f"_{key.lower()}"
                if suffix in seen_suffixes:
                    logger.info(
                        f"MAP '{map_col_name}' chave '{key}' ignorada — sufixo '{suffix}' já processado (conflito de case)"
                    )
                    continue
                seen_suffixes.add(suffix)
                logger.info(
                    f"MAP '{map_col_name}' chave '{key}' contém JSON array → sub-tabela com sufixo '{suffix}'"
                )

                key_df = exploded_df.filter(F.col("_map_key") == key)

                # Infere schema a partir dos valores desta chave específica
                # Spark read.json em arrays: cada elemento do array vira um record
                sample_values = key_df.select("_map_value").limit(100)
                json_rdd = sample_values.rdd.map(lambda row: row["_map_value"])
                inferred_df = spark.read.json(json_rdd)
                inferred_schema = inferred_df.schema

                if not inferred_schema.fields:
                    logger.info(f"  Chave '{key}': não foi possível inferir schema, ignorando")
                    continue

                # Scalar arrays (e.g. code_snippet: ["line1","line2"]) infer as {value: string}
                if {f.name for f in inferred_schema.fields} == {"value"}:
                    logger.info(f"  Chave '{key}': array de escalares (campo 'value' apenas), ignorando")
                    continue

                # O value é um JSON array → usa from_json com ArrayType(schema_do_elemento)
                array_schema = ArrayType(inferred_schema)

                parsed_df = key_df.select(
                    f"{root_id_col.lower()}",
                    parent_id_col,
                    F.from_json(F.col("_map_value"), array_schema).alias("_parsed_array")
                ).filter(F.col("_parsed_array").isNotNull())

                # Explode o array em registros individuais
                final_df = parsed_df.select(
                    f"{root_id_col.lower()}",
                    parent_id_col,
                    F.explode_outer(F.col("_parsed_array")).alias("_nested")
                ).filter(F.col("_nested").isNotNull())

                if final_df.count() == 0:
                    logger.info(f"  Chave '{key}': array vazio após parse, ignorando")
                    continue

                # Expande o struct aninhado
                final_df = final_df.select(
                    f"{root_id_col.lower()}",
                    parent_id_col,
                    "_nested.*"
                )

                final_df = cls.fix_nested_duplicate_names(final_df)
                final_df = final_df.withColumn(
                    "execution_date", F.lit(execution_date).cast("date")
                ).withColumn("_trusted_ingested_at", F.current_timestamp())
                results.append((key, suffix, final_df))

            return results if results else None

        except Exception as e:
            logger.warning(f"Erro ao extrair MAP {map_col_name}: {e}")
            logger.debug(traceback.format_exc())
            return None

    # =========================================================================
    # REMOÇÃO DE CAMPOS EXTRAÍDOS DO DATAFRAME PRINCIPAL
    # =========================================================================

    @classmethod
    def _remove_nested_fields(
        cls,
        df: DataFrame,
        nested_tables_info: List[Tuple[str, str, Optional[StructType]]]
    ) -> DataFrame:
        """
        Remove campos que foram extraídos como tabelas filhas do DataFrame principal.
        - Arrays de Structs e Maps no nível raiz → remove coluna inteira
        - Arrays dentro de Structs → reconstrói struct sem o campo array
        """
        fields_to_remove = {}
        for nested_path, _, _ in nested_tables_info:
            parts = nested_path.split('.')
            if len(parts) > 1:
                parent = parts[0]
                child = parts[-1]
                if parent not in fields_to_remove:
                    fields_to_remove[parent] = []
                fields_to_remove[parent].append(child)
            else:
                fields_to_remove[parts[0]] = None

        if not fields_to_remove:
            return df

        logger.info(f"Removendo campos extraídos: {fields_to_remove}")

        select_exprs = []
        for field in df.schema.fields:
            if field.name in fields_to_remove:
                if fields_to_remove[field.name] is None:
                    if isinstance(field.dataType, MapType):
                        # MAP: mantém a coluna — chaves de array extraídas são filtradas
                        # em run_ingestion via _filter_map_column; escalares permanecem
                        select_exprs.append(F.col(f"`{field.name}`"))
                    else:
                        logger.info(f"  Removendo coluna: {field.name}")
                        continue
                elif isinstance(field.dataType, StructType):
                    children_to_remove = set(fields_to_remove[field.name])
                    struct_fields = []
                    for sub_field in field.dataType.fields:
                        if sub_field.name not in children_to_remove:
                            struct_fields.append(
                                F.col(f"`{field.name}`.`{sub_field.name}`").alias(sub_field.name)
                            )
                    if struct_fields:
                        select_exprs.append(F.struct(*struct_fields).alias(field.name))
                    else:
                        logger.info(f"  Struct '{field.name}' vazio após remoção → removido")
                else:
                    logger.info(f"  Removendo coluna: {field.name}")
                    continue
            else:
                select_exprs.append(F.col(f"`{field.name}`"))

        return df.select(*select_exprs)

    # =========================================================================
    # DESCOBERTA E FILTRAGEM DE CHAVES MAP
    # =========================================================================

    @classmethod
    def _discover_map_array_keys(cls, df: DataFrame, map_col_name: str) -> List[str]:
        """
        Retorna as chaves distintas de um MAP<STRING,STRING> cujos valores são
        JSON arrays (começam com '[').  Usado para saber quais chaves serão
        extraídas como subtabelas antes de construir o df_main.
        """
        try:
            path_parts = map_col_name.split('.')
            col_ref = (
                F.col(f"`{path_parts[0]}`.`{path_parts[-1]}`")
                if len(path_parts) > 1
                else F.col(f"`{map_col_name}`")
            )
            array_keys = (
                df.select(F.explode_outer(col_ref).alias("_k", "_v"))
                .filter(
                    F.col("_v").isNotNull()
                    & F.trim(F.col("_v")).rlike(r'^\[\s*\{')
                )
                .select("_k")
                .distinct()
                .collect()
            )
            return [r["_k"] for r in array_keys]
        except Exception as e:
            logger.warning(f"Erro ao descobrir chaves array no MAP '{map_col_name}': {e}")
            return []

    @staticmethod
    def _filter_map_column(df: DataFrame, map_col: str, keys_to_remove: List[str]) -> DataFrame:
        """
        Remove chaves específicas de um MAP<STRING,STRING>, mantendo todas as
        demais (escalares).  Usa map_filter disponível no Spark 3.0+.
        """
        if not keys_to_remove:
            return df
        return df.withColumn(
            map_col,
            F.map_filter(F.col(f"`{map_col}`"), lambda k, v: ~k.isin(keys_to_remove))
        )

    # =========================================================================
    # SINCRONIZAÇÃO DE SCHEMA
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
        """Adiciona ao Iceberg colunas que existem no DataFrame mas não na tabela."""
        try:
            table_schema = spark.table(table_identifier).schema
            existing_cols = {f.name.lower() for f in table_schema.fields}
            new_cols = [f for f in df.schema.fields if f.name.lower() not in existing_cols]

            if not new_cols:
                logger.info(f"Schema de {table_identifier} já sincronizado")
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

    # =========================================================================
    # ALINHAMENTO DO DATAFRAME COM A TABELA
    # =========================================================================

    @classmethod
    def rebuild_aligned_struct(cls, col_expr, df_struct_type, table_struct_type, col_name):
        """Reconstrói um struct para alinhar com o schema da tabela."""
        fields = []
        df_fields_map = {f.name.lower(): f for f in df_struct_type.fields}

        for table_field in table_struct_type.fields:
            df_field = df_fields_map.get(table_field.name.lower())

            if df_field:
                field_col = col_expr[df_field.name]
                if isinstance(table_field.dataType, StructType) and isinstance(df_field.dataType, StructType):
                    nested = cls.rebuild_aligned_struct(
                        field_col, df_field.dataType, table_field.dataType, df_field.name
                    )
                    fields.append(nested.alias(table_field.name))
                else:
                    fields.append(field_col.cast(table_field.dataType).alias(table_field.name))
            else:
                fields.append(F.lit(None).cast(table_field.dataType).alias(table_field.name))

        return F.struct(*fields)

    @classmethod
    def align_dataframe_to_table(cls, df: DataFrame, table_schema: StructType) -> DataFrame:
        """Alinha o DataFrame com o schema da tabela (colunas ausentes → null, ordem correta)."""
        select_exprs = []
        df_fields_map = {f.name.lower(): f for f in df.schema.fields}

        for table_field in table_schema.fields:
            df_field = df_fields_map.get(table_field.name.lower())

            if df_field:
                col_expr = F.col(f"`{df_field.name}`")

                if isinstance(table_field.dataType, StringType) and isinstance(
                    df_field.dataType, (StructType, ArrayType, MapType)
                ):
                    select_exprs.append(F.to_json(col_expr).alias(table_field.name))
                elif isinstance(table_field.dataType, StructType) and isinstance(df_field.dataType, StructType):
                    rebuilt = cls.rebuild_aligned_struct(
                        col_expr, df_field.dataType, table_field.dataType, df_field.name
                    )
                    select_exprs.append(rebuilt.alias(table_field.name))
                elif (
                    isinstance(df_field.dataType, MapType)
                    and isinstance(table_field.dataType, StructType)
                ):
                    select_exprs.append(F.lit(None).cast(table_field.dataType).alias(table_field.name))
                else:
                    select_exprs.append(col_expr.cast(table_field.dataType).alias(table_field.name))
            else:
                select_exprs.append(F.lit(None).cast(table_field.dataType).alias(table_field.name))

        return df.select(*select_exprs)

    # =========================================================================
    # CRIAÇÃO / ESCRITA EM TABELA ICEBERG
    # =========================================================================

    @classmethod
    def create_or_append_table(
        cls,
        spark: SparkSession,
        df: DataFrame,
        table_identifier: str,
        partition_by: List[str],
        write_mode: str = "append",
        options: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Cria tabela Iceberg ou faz append com sincronização de schema.
        Usa days(_trusted_ingested_at) como partição principal.
        """
        table_exists = spark.catalog.tableExists(table_identifier)

        if table_exists:
            logger.info(f"Tabela {table_identifier} existe — sincronizando schema")

            cls.sync_schema(spark, df, table_identifier)

            table_schema = spark.table(table_identifier).schema
            df_aligned = cls.align_dataframe_to_table(df, table_schema)
            logger.info(f"DataFrame alinhado: {len(df_aligned.columns)} colunas")

            writer = df_aligned.writeTo(table_identifier)
            writer = writer.option("mergeSchema", "true").option("check-ordering", "false")

            if options:
                writer = writer.options(**options)

            if write_mode == "overwrite":
                writer.overwritePartitions()
            else:
                writer.append()
        else:
            logger.info(f"Criando tabela {table_identifier}")

            # Cria usando writeTo API com days partition transform
            empty_df = spark.createDataFrame([], df.schema)
            writer = empty_df.writeTo(table_identifier) \
                .tableProperty("format-version", "2") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("write.parquet.compression-codec", "snappy")

            if partition_by:
                writer = writer.partitionedBy(*partition_by)
                logger.info(f"Particionamento: {partition_by}")

            writer.createOrReplace()
            logger.info(f"Tabela {table_identifier} criada com sucesso")

            # Agora escreve os dados
            df.writeTo(table_identifier).option("check-ordering", "false").append()

        spark.catalog.refreshTable(table_identifier)
        logger.info(f"Escrita concluída em {table_identifier}")

    # =========================================================================
    # PIPELINE PRINCIPAL
    # =========================================================================

    @classmethod
    def run_ingestion(
        cls,
        spark: SparkSession,
        execution_date: str,
        config: Dict[str, Any]
    ) -> None:
        """
        Executa a ingestão normalizada de dados para tabelas Iceberg TRUSTED.

        Fluxo:
          1. Lê da tabela RAW filtrando por partition_date_by
          2. Trata conflitos de nomes (Id vs id)
          3. Identifica tabelas filhas (Arrays + Maps)
          4. Cria/atualiza tabela principal (sem campos extraídos)
          5. Cria/atualiza tabelas filhas

        Args:
            spark: SparkSession ativa
            execution_date: Data no formato YYYY-MM-DD
            config: Configuração com source e destination
        """
        # =====================================================================
        # ETAPA 1: Ler dados da tabela RAW
        # =====================================================================
        logger.info("=" * 60)
        logger.info("TRUSTED ICEBERG WRITER")
        logger.info(f"ExecuteDate: {execution_date}")
        logger.info("=" * 60)

        logger.info("\nETAPA 1: Buscar dados da tabela RAW")

        database_name = config["source"]["database_name"]
        table_name = config["source"]["table_name"]
        partition_date_by = config["source"].get("partition_date_by", "_raw_ingested_at")
        source_table = f"{database_name}.{table_name}"

        logger.info(f"Tabela fonte: {source_table} | Partição: {partition_date_by}")

        df_raw = cls.read_source_table(spark, source_table, partition_date_by, execution_date)
        if df_raw is None:
            logger.warning("Nenhum dado encontrado na tabela fonte")
            return

        record_count = df_raw.count()
        if record_count == 0:
            logger.warning("Nenhum registro para processar")
            return

        logger.info(f"Registros: {record_count} | Colunas: {len(df_raw.columns)}")

        # =====================================================================
        # ETAPA 2: Tratar conflitos case-sensitive (Id vs id)
        # =====================================================================
        logger.info("\nETAPA 2: Tratar conflitos de nomes")
        df_raw = cls.fix_duplicate_column_names(df_raw)

        # Adiciona _trusted_ingested_at
        df_raw = df_raw.withColumn("_trusted_ingested_at", F.current_timestamp())

        # =====================================================================
        # ETAPA 3: Identificar tabelas filhas
        # =====================================================================
        dest = config["destination"]
        catalog = dest["catalog_name"]
        database = dest["database_name"]
        main_table = dest["main_table_name"]
        main_table_identifier = f"{catalog}.{database}.{main_table}"
        partition_by = dest.get("partition_by", [])

        logger.info("\nETAPA 3: Identificar estruturas para normalização")
        logger.info("-" * 60)

        nested_tables_info = cls.identify_nested_tables(main_table, df_raw.schema)

        if not nested_tables_info:
            logger.info("Nenhuma estrutura semi-estruturada detectada")
        else:
            logger.info(f"{len(nested_tables_info)} tabela(s) filha(s) identificada(s):")
            for path, tname, _ in nested_tables_info:
                logger.info(f"  {path} → {tname}")

        # Pré-descoberta: identifica quais chaves de cada MAP virarão subtabelas.
        # Necessário para filtrar o MAP no df_main antes de escrevê-lo.
        map_keys_to_filter: Dict[str, List[str]] = {}
        for nested_path, _, _ in nested_tables_info:
            col_name = nested_path.split('.')[0]
            field_obj = next(
                (f for f in df_raw.schema.fields if f.name == col_name), None
            )
            if field_obj and isinstance(field_obj.dataType, MapType):
                if col_name not in map_keys_to_filter:
                    logger.info(f"  Descobrindo chaves array no MAP '{col_name}'...")
                    keys = cls._discover_map_array_keys(df_raw, col_name)
                    map_keys_to_filter[col_name] = keys
                    logger.info(f"  MAP '{col_name}': {len(keys)} chave(s) com array encontrada(s)")

        # =====================================================================
        # ETAPA 4: Criar/atualizar tabela principal
        # =====================================================================
        logger.info("\nETAPA 4: Criar/atualizar tabela principal")

        df_main = cls._remove_nested_fields(df_raw, nested_tables_info)

        # Filtra chaves extraídas de cada MAP, mantendo os valores escalares
        for map_col, keys in map_keys_to_filter.items():
            if keys:
                df_main = cls._filter_map_column(df_main, map_col, keys)
                logger.info(f"  MAP '{map_col}': {len(keys)} chave(s) de array removida(s) — escalares mantidos")

        logger.info(f"Tabela principal: {main_table_identifier}")
        logger.info(f"Colunas: {len(df_main.columns)}")

        cls.create_or_append_table(
            spark=spark,
            df=df_main,
            table_identifier=main_table_identifier,
            partition_by=partition_by,
            write_mode=dest["write_mode"],
            options=dest.get("options")
        )

        # =====================================================================
        # ETAPA 5: Criar/atualizar tabelas filhas
        # =====================================================================
        if nested_tables_info:
            logger.info("\nETAPA 5: Criar/atualizar tabelas filhas")

            # Identifica coluna ID principal
            id_columns = [
                c for c in df_raw.columns
                if c.lower().endswith('id')
                and c.lower() not in ('execution_date', '_trusted_ingested_at', '_raw_ingested_at')
            ]
            root_id_col = id_columns[0] if id_columns else df_raw.columns[0]
            logger.info(f"Coluna ID raiz: {root_id_col}")

            for nested_path, child_table_name, nested_schema in nested_tables_info:
                logger.info(f"\nProcessando: {nested_path} → {child_table_name}")

                # Detecta tipo do campo para escolher método de extração
                is_map_field = False
                map_results = None
                nested_df = None
                path_parts = nested_path.split('.')
                if len(path_parts) > 1:
                    parent_name = path_parts[0]
                    parent_field = next(
                        (f for f in df_raw.schema.fields if f.name == parent_name), None
                    )
                    if parent_field and isinstance(parent_field.dataType, MapType):
                        # Campo é MAP → extração especial
                        is_map_field = True
                        map_results = cls.extract_nested_from_map(
                            spark, df_raw, nested_path, root_id_col, execution_date
                        )
                    else:
                        nested_df = cls.extract_nested_from_array(
                            df_raw, nested_path, root_id_col, execution_date
                        )
                else:
                    # Campo raiz
                    field_obj = next(
                        (f for f in df_raw.schema.fields if f.name == path_parts[0]), None
                    )
                    if field_obj and isinstance(field_obj.dataType, MapType):
                        is_map_field = True
                        map_results = cls.extract_nested_from_map(
                            spark, df_raw, nested_path, root_id_col, execution_date
                        )
                    else:
                        is_map_field = False
                        nested_df = cls.extract_nested_from_array(
                            df_raw, nested_path, root_id_col, execution_date
                        )

                # Tratamento para campos MAP (retorna lista de sub-tabelas)
                if is_map_field:
                    if not map_results:
                        logger.warning(f"Nenhum dado para {nested_path}")
                        continue

                    for original_key, suffix, sub_df in map_results:
                        sub_table_name = f"{child_table_name}{suffix}"
                        sub_table_identifier = f"{catalog}.{database}.{sub_table_name}"
                        logger.info(f"Registros extraídos: {sub_df.count()} → {sub_table_name}")

                        cls.create_or_append_table(
                            spark=spark,
                            df=sub_df,
                            table_identifier=sub_table_identifier,
                            partition_by=partition_by,
                            write_mode=dest["write_mode"],
                            options=dest.get("options")
                        )
                else:
                    if nested_df is None or nested_df.count() == 0:
                        logger.warning(f"Nenhum dado para {nested_path}")
                        continue

                    logger.info(f"Registros extraídos: {nested_df.count()}")

                    child_table_identifier = f"{catalog}.{database}.{child_table_name}"

                    cls.create_or_append_table(
                        spark=spark,
                        df=nested_df,
                        table_identifier=child_table_identifier,
                        partition_by=partition_by,
                        write_mode=dest["write_mode"],
                        options=dest.get("options")
                    )

            logger.info("\nTodas as tabelas filhas processadas")

        logger.info("=" * 60)
        logger.info(f"TRUSTED carregada: {main_table_identifier}")
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