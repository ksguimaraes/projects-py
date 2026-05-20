import argparse
import logging
from typing import Any, Dict
from urllib.parse import urlparse

import boto3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

"""
    Job Glue ETL para ingestão de tabelas TRS legadas.
    Lê CSV sem cabeçalho do S3, escreve em Iceberg e ORC (Hive legado),
    e move os arquivos processados para o bucket de destino.
"""

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TrsIngestion:
    """Classe responsável pela ingestão de tabelas TRS legadas."""

    @staticmethod
    def has_files_to_process(bucket: str, prefix: str, tablename: str) -> bool:
        """Verifica se existem arquivos CSV para processar no S3."""
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=f"{prefix}/{tablename}",
        )
        objects = response.get("Contents")
        if not objects:
            return False
        return any(obj["Key"].endswith(".csv") for obj in objects)

    @staticmethod
    def move_csv_files(
        source_bucket: str,
        source_prefix: str,
        dest_bucket: str,
        dest_prefix: str,
        tablename: str,
    ) -> None:
        """Move arquivos CSV do source para o destino usando boto3."""
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        moved = 0

        for page in paginator.paginate(Bucket=source_bucket, Prefix=f"{source_prefix}/{tablename}"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".csv"):
                    continue
                filename = key.split("/")[-1]
                dest_key = f"{dest_prefix}/{tablename}/{filename}"
                s3.copy_object(
                    CopySource={"Bucket": source_bucket, "Key": key},
                    Bucket=dest_bucket,
                    Key=dest_key,
                )
                s3.delete_object(Bucket=source_bucket, Key=key)
                moved += 1
                logger.info(f"Movido: s3://{source_bucket}/{key} -> s3://{dest_bucket}/{dest_key}")

        logger.info(f"{moved} arquivo(s) CSV movido(s).")

    @staticmethod
    def create_success_file(orc_location: str) -> None:
        """Cria arquivo _SUCCESS no diretório ORC após a escrita, replicando comportamento Hadoop."""
        s3 = boto3.client("s3")
        parsed = urlparse(orc_location.rstrip("/"))
        bucket = parsed.netloc
        key = f"{parsed.path.lstrip('/')}/_SUCCESS"
        s3.put_object(Bucket=bucket, Key=key, Body=b"")
        logger.info(f"Arquivo _SUCCESS criado em s3://{bucket}/{key}")

    @classmethod
    def read_csv(cls, spark: SparkSession, s3_path: str, legacy_table: str) -> DataFrame:
        """Lê CSVs sem cabeçalho usando o schema obtido da tabela legada no catálogo Glue."""
        schema = spark.sql(f"SELECT * FROM {legacy_table} LIMIT 0").schema
        logger.info(f"Schema obtido de {legacy_table}: {schema.simpleString()}")
        logger.info(f"Lendo CSV de: {s3_path}")
        return (
            spark.read.format("csv")
            .option("escape", '"')
            .option("encoding", "UTF-8")
            .option("header", "false")
            .option("delimiter", ";")
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .load(s3_path)
        )

    @classmethod
    def write_iceberg(cls, df: DataFrame, table_identifier: str) -> None:
        """Substitui todos os dados da tabela Iceberg (full load)."""
        logger.info(f"Escrevendo dados na tabela Iceberg: {table_identifier}")
        df.writeTo(table_identifier).overwrite(F.lit(True))
        logger.info("Escrita Iceberg concluída.")

    @classmethod
    def write_hive_orc(cls, df: DataFrame, orc_location: str) -> None:
        """Escreve dados no path ORC da tabela Hive legada."""
        logger.info(f"Escrevendo dados em ORC: {orc_location}")
        df.write.mode("overwrite").orc(orc_location)
        logger.info("Escrita ORC concluída.")

    @classmethod
    def run_ingestion(cls, spark: SparkSession, config: Dict[str, Any]) -> None:
        """Executa o pipeline completo: leitura CSV → Iceberg → ORC → move arquivos."""
        tablename = config["tablename"]
        source_bucket = config["source_bucket"]
        source_prefix = config["source_prefix"]
        dest_bucket = config["dest_bucket"]
        dest_prefix = config["dest_prefix"]
        orc_location = config["orc_location"]
        iceberg_database = config.get("iceberg_database", "trs")

        if not cls.has_files_to_process(source_bucket, source_prefix, tablename):
            logger.warning(f"Nenhum arquivo CSV encontrado para '{tablename}'. Finalizando.")
            return

        table_identifier = f"glue_catalog.{iceberg_database}.{tablename}"
        legacy_table = f"trs.{tablename}_legacy"
        s3_source_path = f"s3://{source_bucket}/{source_prefix}/{tablename}/*.csv"

        df = cls.read_csv(spark, s3_source_path, legacy_table)
        df.persist()

        record_count = df.count()
        logger.info(f"Total de registros lidos: {record_count}")

        if record_count == 0:
            logger.warning("DataFrame vazio após leitura. Finalizando.")
            df.unpersist()
            return

        cls.write_iceberg(df, table_identifier)
        cls.write_hive_orc(df, orc_location)
        cls.create_success_file(orc_location)
        cls.move_csv_files(source_bucket, source_prefix, dest_bucket, dest_prefix, tablename)

        df.unpersist()
        logger.info(f"Ingestão concluída com sucesso. {record_count} registros processados.")


def create_spark_session() -> SparkSession:
    """Retorna a SparkSession existente criada pelo Glue."""
    return SparkSession.builder.appName("TRS Legacy Tables Ingestion").getOrCreate()


def configure_spark(spark: SparkSession, warehouse: str) -> None:
    """
    Aplica configurações do catálogo Iceberg via spark.conf.set após a sessão existir.
    No Glue, SparkSession.builder.config(...) é ignorado — a sessão já existe.
    """
    confs = {
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.glue_catalog.warehouse": warehouse,
        "spark.sql.shuffle.partitions": "20",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
    }
    for key, value in confs.items():
        spark.conf.set(key, value)


def parse_arguments() -> argparse.Namespace:
    """Analisa os argumentos de linha de comando."""
    parser = argparse.ArgumentParser(
        description="Ingestão de tabelas TRS legadas em Iceberg e ORC via Glue ETL.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--tablename",
        type=str,
        required=True,
        help="Nome da tabela TRS sem schema (ex: admins).",
    )
    parser.add_argument(
        "--source-bucket",
        type=str,
        required=True,
        help="Bucket S3 de origem dos CSVs.",
    )
    parser.add_argument(
        "--source-prefix",
        type=str,
        required=True,
        help="Prefixo S3 de origem dos CSVs (ex: olx/stg/trs/toload/fulltables).",
    )
    parser.add_argument(
        "--dest-bucket",
        type=str,
        required=True,
        help="Bucket S3 de destino após processamento.",
    )
    parser.add_argument(
        "--dest-prefix",
        type=str,
        required=True,
        help="Prefixo S3 de destino após processamento (ex: olx/stg/trs/loaded/fulltables).",
    )
    parser.add_argument(
        "--orc-location",
        type=str,
        required=True,
        help="Caminho S3 completo para escrita ORC da tabela Hive (ex: s3://bucket/olx/trs/admins).",
    )
    parser.add_argument(
        "--warehouse",
        type=str,
        required=True,
        help="Caminho S3 do warehouse Iceberg.",
    )
    parser.add_argument(
        "--iceberg-database",
        type=str,
        default="trs",
        help="Nome do database Iceberg no Glue Catalog (default: trs).",
    )

    args, _ = parser.parse_known_args()
    return args


def main():
    """Função principal do job Glue."""
    args = parse_arguments()
    spark = None
    try:
        spark = create_spark_session()
        configure_spark(spark, args.warehouse)
        logger.info("SparkSession configurada com sucesso.")

        config = {
            "tablename": args.tablename,
            "source_bucket": args.source_bucket,
            "source_prefix": args.source_prefix,
            "dest_bucket": args.dest_bucket,
            "dest_prefix": args.dest_prefix,
            "orc_location": args.orc_location,
            "iceberg_database": args.iceberg_database,
        }
        logger.info(f"Configuração: {config}")

        TrsIngestion.run_ingestion(spark, config)
        logger.info("Job finalizado com sucesso.")

    except Exception as e:
        logger.error(f"Erro durante a execução do job: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession encerrada.")


if __name__ == "__main__":
    main()
