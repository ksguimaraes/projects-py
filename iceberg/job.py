import argparse
from datetime import datetime, timedelta
import json
import logging
import re
from typing import Any, Union, List, Dict

from pyspark.sql.functions import (
    col, row_number, when, lit, current_timestamp
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column
import boto3
from urllib.parse import urlparse

"""
    Job Spark para processamento de tabelas trusted de transações
"""

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IcebergIngestion:
    """Classe responsável pela ingestão de dados em tabelas Iceberg."""

    @staticmethod
    def fetch_s3_file(s3_path: str) -> str:
        """Baixa o conteúdo de um arquivo do S3 e retorna como string."""
        s3 = boto3.client("s3")
        parsed = urlparse(s3_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")

    @classmethod
    def load_query(cls, spark: SparkSession, query_path: str, start_date: str, end_date: str) -> DataFrame:
        """Carrega a consulta SQL de um arquivo local ou S3, substitui placeholders e executa para obter um DataFrame."""
        logger.info(f"Carregando consulta SQL de: {query_path}")
        try:
            if query_path.startswith("s3://"):
                query = cls.fetch_s3_file(query_path)
            else:
                with open(query_path, 'r') as file:
                    query = file.read()

            query = query.replace("{start_date}", start_date).replace("{end_date}", end_date)

            logger.info(f"Consulta SQL carregada e placeholders substituídos (START_DATE={start_date}, END_DATE={end_date}). Executando consulta...")
            return spark.sql(query)
        except Exception as e:
            logger.error(f"Erro ao carregar ou executar a consulta SQL: {e}")
            raise

    @staticmethod
    def strip_quotes(value: str) -> str:
        """Remove aspas simples ou duplas do início e fim de uma string."""
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            return value[1:-1]
        return value

    @classmethod
    def parse_partition_expression(cls, partition_expression: Union[str, Column]) -> Column:
        """Converte uma expressão de partição em uma coluna Spark.
        
        Args:
            partition_expression: String com expressão de partição ou Column Spark.
            
        Returns:
            Column Spark correspondente à expressão.
            
        Raises:
            ValueError: Se a expressão for inválida ou não suportada.
        """
        if isinstance(partition_expression, Column):
            return partition_expression

        if not isinstance(partition_expression, str) or not partition_expression.strip():
            raise ValueError(
                "partition_by deve conter strings válidas ou colunas Spark. "
                f"Valor recebido: {partition_expression!r}"
            )

        expression = partition_expression.strip()
        transform_match = re.fullmatch(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*)\)", expression)

        if not transform_match:
            return F.col(cls.strip_quotes(expression))

        transform_name = transform_match.group(1).lower()
        args = [
            cls.strip_quotes(argument.strip())
            for argument in transform_match.group(2).split(",")
            if argument.strip()
        ]

        if transform_name in {"years", "months", "days", "hours"}:
            if len(args) != 1:
                raise ValueError(
                    f"Transformação '{transform_name}' requer 1 argumento: {partition_expression}"
                )
            return getattr(F, transform_name)(args[0])

        if transform_name == "bucket":
            if len(args) != 2:
                raise ValueError(
                    f"Transformação 'bucket' requer 2 argumentos: {partition_expression}"
                )
            buckets = int(args[0])
            return F.bucket(buckets, args[1])

        if transform_name == "identity":
            if len(args) != 1:
                raise ValueError(
                    f"Transformação 'identity' requer 1 argumento: {partition_expression}"
                )
            return F.col(args[0])

        raise ValueError(
            "Transformação de partição não suportada em partition_by: "
            f"{partition_expression}."
        )

    @classmethod
    def build_partition_spec(cls, partition_by: List[Union[str, Column]]) -> List[Column]:
        """Constrói a especificação de partições a partir de uma lista de expressões."""
        return [cls.parse_partition_expression(expr) for expr in partition_by]

    @classmethod
    def sync_schema(cls, spark: SparkSession, df, table_identifier: str) -> None:
        """Adiciona colunas novas à tabela se necessário."""        
        # Pegamos o schema da tabela e do DF
        table_schema = spark.table(table_identifier).schema
        df_schema = df.schema
        
        existing_cols = {f.name.lower() for f in table_schema.fields}
        new_cols = [f for f in df_schema.fields if f.name.lower() not in existing_cols]
        
        for field in new_cols:
            # O Iceberg aceita tipos do Spark via SQL, mas evite tipos nulos complexos aqui
            dtype = field.dataType.simpleString()
            try:
                logger.info(f"Tentando adicionar coluna: {field.name} ({dtype})")
                spark.sql(f"ALTER TABLE {table_identifier} ADD COLUMN {field.name} {dtype}")
            except Exception as e:
                logger.error(f"Erro ao evoluir schema para a coluna {field.name}: {e}")

    @classmethod
    def merge_data(cls, spark: SparkSession, df: DataFrame, table_identifier: str, primary_key: List[str], timestamp_column: str) -> None:
        """Realiza merge dos dados usando SQL com base na chave primária."""       
        condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_key])
        update_condition = f"source.{timestamp_column} >= target.{timestamp_column}"
        merge_sql = f"""
            MERGE INTO {table_identifier} AS target
            USING (SELECT * FROM staging_table) AS source
            ON {condition}
            WHEN MATCHED AND {update_condition} THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """
        df.createOrReplaceTempView("staging_table")
        try:
            logger.info(f"Iniciando merge na tabela {table_identifier} usando as chaves primárias: {primary_key}.")
            spark.sql(merge_sql)
        except Exception as e:
            logger.error(f"Erro durante o merge na tabela {table_identifier}: {e}")
            raise

    @classmethod
    def run_ingestion(cls, spark: SparkSession, config: Dict[str, Any], start_date: str, end_date: str) -> None:
        """
        Job genérico de injeção raw para tabelas Iceberg.
        
        write_mode suportados:
            - append: adiciona dados
            - overwrite: sobrescreve partições
        """        
        
        # Carrega os dados usando a consulta SQL definida no arquivo
        query_path = config["source"]["query_path"]
        df = cls.load_query(spark, query_path, start_date=start_date, end_date=end_date)
        dest = config["destination"]
        merge_config = dest["options"].get("merge_config", {})
        primary_key = merge_config.get("primary_key")
        if primary_key:
            df = df.drop_duplicates(primary_key)  

        record_count = df.count()
        logger.info(f"Total de registros lidos: {record_count}")
        
        if record_count == 0:
            logger.warning("Nenhum registro encontrado para processar. Finalizando job.")
            return

        table_identifier = f"glue_catalog.{dest['database_name']}.{dest['table_name']}"
        table_exists = spark.catalog.tableExists(table_identifier)
        
        if table_exists:
            logger.info(f"Tabela {table_identifier} existe. Verificando compatibilidade de schema.")        
            # Sincroniza schema antes de escrever para evitar erros de schema mismatch
            cls.sync_schema(spark, df, table_identifier)

            table_schema_df = spark.table(table_identifier).limit(0)
            # Garante que o DataFrame de leitura tenha todas as colunas da tabela (mesmo que vazias) para evitar erros de schema
            df = df.unionByName(table_schema_df, allowMissingColumns=True)
        
        writer = df.writeTo(table_identifier).tableProperty("format-version", "2")
        logger.info(f"Escrevendo dados em: {table_identifier}")
        
        if "options" in dest:
            writer = writer.options(**dest["options"])
        
        write_mode = dest.get("write_mode", "append")

        if table_exists:
            if write_mode == "append":
                logger.info("Modo append: adicionando dados à tabela existente.")
                writer.append()
            elif write_mode == "overwrite":
                logger.info("Modo overwrite: sobrescrevendo partições.")
                writer.overwritePartitions()
            elif write_mode == "merge":
                logger.info("Modo merge: realizando merge com base nas chaves primárias.")
                
                if not primary_key:
                    raise ValueError("Configuração de merge inválida: 'primary_key' é obrigatório.")

                timestamp_column = merge_config.get("timestamp_column")

                cls.merge_data(spark, df, table_identifier, primary_key, timestamp_column)
            else:
                raise ValueError(f"write_mode '{write_mode}' não suportado para tabela existente.")
        else:
            logger.info("Tabela não existe. Criando nova tabela.")
            # Particionamento dinâmico
            partition_by = dest.get("partition_by")
            if partition_by:
                writer = writer.partitionedBy(*cls.build_partition_spec(partition_by))
                logger.info(f"Particionado por: {partition_by}")
            writer.createOrReplace()
        
        spark.catalog.refreshTable(table_identifier)

        logger.info(f"Ingestão concluída com sucesso. {record_count} registros processados.")

def create_spark_session(warehouse: str) -> SparkSession:
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
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            "spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.iceberg.spark.Spark3Util$IcebergCommitProtocol",
        )
        .config("spark.sql.catalog.glue_catalog.warehouse", warehouse)
        # Configurações do S3FileIO do Iceberg (SDK AWS v2) - valores altos para tabelas grandes
        .config("spark.sql.catalog.glue_catalog.s3.max-connections", "500")
        .config("spark.sql.catalog.glue_catalog.s3.connection-timeout-ms", "120000")
        .config("spark.sql.catalog.glue_catalog.s3.socket-timeout-ms", "120000")
        .config("spark.sql.catalog.glue_catalog.s3.request-timeout-ms", "300000")
        .config("spark.sql.catalog.glue_catalog.s3.connection-acquisition-timeout-ms", "120000")
        # Configurações de retry do cliente S3
        .config("spark.sql.catalog.glue_catalog.client.retry.num-retries", "10")
        # Configurações de paralelismo reduzido para evitar esgotamento do pool
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.default.parallelism", "50")
        # Reduz threads de commit do Iceberg
        .config("spark.sql.catalog.glue_catalog.s3.write.max-workers", "4")
        # Habilita Adaptive Query Execution para otimizar automaticamente
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "10")
        # Configurações do SDK AWS para o Iceberg
        .config("spark.hadoop.fs.s3a.connection.maximum", "500")
        .config("spark.hadoop.fs.s3a.threads.max", "50")
        .config("spark.hadoop.fs.s3a.connection.timeout", "120000")
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
        '--path-query',
        type=str,
        required=True,
        help='Caminho do arquivo SQL no S3 para leitura dos dados'
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
    
    spark = None
    try:
        # Parse das configurações
        config = json.loads(args.config)
        logger.info(f"Configuração carregada: {json.dumps(config, indent=2)}")
        
        start_date = config.get("start_date", "")
        end_date = config.get("end_date", "")
        if not start_date or not end_date:
            # calcula start_date e end_date com base na execution_date 
            # start_date = execution_date e end_date = execution_date + 1 dia
            execution_date_dt = datetime.strptime(args.execute_date, "%Y-%m-%d")
            start_date = execution_date_dt.strftime("%Y-%m-%d")
            end_date = (execution_date_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    
        logger.info(f"Iniciando job para data: {start_date} até {end_date}")

        spark = create_spark_session(config.get("destination", {}).get("warehouse"))
        logger.info("SparkSession criada com sucesso.")
        logger.info(f"Caminho do arquivo SQL: {args.path_query}")
        config["source"] = {"query_path": args.path_query}

        # Executa a ingestão
        IcebergIngestion.run_ingestion(spark, config, start_date, end_date)
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