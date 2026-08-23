"""
Streaming Silver — Glue Job (PySpark).

Lê os eventos persistidos pela Lambda em:
    s3://<bucket>/bronze/streaming/

Aplica:
- tipagem explícita;
- normalização de timestamp;
- validação básica;
- deduplicação por event_id.

Grava:
    s3://<bucket>/silver/streaming/eventos/
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "BUCKET_DESTINO", "ENV"],
    )

    contexto = GlueContext(SparkContext.getOrCreate())
    spark = contexto.spark_session

    job = Job(contexto)
    job.init(args["JOB_NAME"], args)

    logger = contexto.get_logger().info

    bucket = args["BUCKET_DESTINO"]

    origem = f"s3://{bucket}/bronze/streaming/"
    destino = f"s3://{bucket}/silver/streaming/eventos/"

    logger("=" * 60)
    logger(f"STREAMING SILVER — ambiente {args['ENV']}")
    logger("=" * 60)
    logger(f"Origem: {origem}")
    logger(f"Destino: {destino}")

    eventos = spark.read.parquet(origem)

    logger(f"Eventos recebidos: {eventos.count():,}")

    silver = (
        eventos
        .select(
            F.col("event_id").cast("string").alias("event_id"),
            F.col("event_type").cast("string").alias("event_type"),
            F.to_timestamp("event_timestamp").alias("event_timestamp"),
            F.col("municipio_id").cast("string").alias("municipio_id"),
            F.col("indicador").cast("string").alias("indicador"),
            F.col("valor").cast("double").alias("valor"),
        )
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("municipio_id").isNotNull())
        .filter(F.col("indicador").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
        .filter(F.col("valor").isNotNull())
        .withColumn("data_evento", F.to_date("event_timestamp"))
    )

    # Mantém apenas a versão mais recente de cada evento.
    janela = Window.partitionBy("event_id").orderBy(
        F.col("event_timestamp").desc()
    )

    silver = (
        silver
        .withColumn("ordem_evento", F.row_number().over(janela))
        .filter(F.col("ordem_evento") == 1)
        .drop("ordem_evento")
    )

    silver = silver.select(
        "event_id",
        "event_type",
        "event_timestamp",
        "data_evento",
        "municipio_id",
        "indicador",
        "valor",
    )

    silver.coalesce(1).write.mode("overwrite").parquet(destino)

    logger(f"Eventos Silver: {silver.count():,}")
    logger(f"Gravado: {destino}")
    logger("STREAMING SILVER CONCLUIDA")

    job.commit()


if __name__ == "__main__":
    main()