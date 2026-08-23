"""
Streaming Gold — Glue Job (PySpark).

Lê:
    s3://<bucket>/silver/streaming/eventos/

Produz:
    s3://<bucket>/gold/streaming/ultimo_indicador_municipio/

Regra:
- um registro por município + indicador;
- permanece somente o evento mais recente.
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

    origem = f"s3://{bucket}/silver/streaming/eventos/"
    destino = f"s3://{bucket}/gold/streaming/ultimo_indicador_municipio/"

    logger("=" * 60)
    logger(f"STREAMING GOLD — ambiente {args['ENV']}")
    logger("=" * 60)
    logger(f"Origem: {origem}")
    logger(f"Destino: {destino}")

    eventos = spark.read.parquet(origem)

    logger(f"Eventos Silver: {eventos.count():,}")

    janela = Window.partitionBy(
        "municipio_id",
        "indicador",
    ).orderBy(
        F.col("event_timestamp").desc(),
        F.col("event_id").desc(),
    )

    gold = (
        eventos
        .withColumn("ordem_atualizacao", F.row_number().over(janela))
        .filter(F.col("ordem_atualizacao") == 1)
        .drop("ordem_atualizacao")
        .select(
            "municipio_id",
            "indicador",
            "valor",
            "event_timestamp",
            "event_id",
            "event_type",
        )
    )

    gold.coalesce(1).write.mode("overwrite").parquet(destino)

    logger(f"Municípios/indicadores atuais: {gold.count():,}")
    logger(f"Gravado: {destino}")
    logger("STREAMING GOLD CONCLUIDA")

    job.commit()


if __name__ == "__main__":
    main()