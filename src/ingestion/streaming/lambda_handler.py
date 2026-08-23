import base64
import io
import json

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import datetime

s3 = boto3.client("s3")

BUCKET = os.environ["AWS_BUCKET"]
PREFIX = os.getenv("STREAMING_PREFIX", "bronze/streaming/")


def lambda_handler(event, context):
    eventos = []

    for record in event["Records"]:
        data = base64.b64decode(
            record["kinesis"]["data"]
        ).decode("utf-8")

        evento = json.loads(data)

        eventos.append(
            {
                "event_id": evento["event_id"],
                "event_type": evento["event_type"],
                "event_timestamp": evento["event_timestamp"],
                "municipio_id": str(evento["municipio_id"]),
                "indicador": evento["indicador"],
                "valor": float(evento["valor"]),
            }
        )

    if not eventos:
        print("[LAMBDA] Nenhum evento recebido.")
        return {"processados": 0}

    tabela = pa.Table.from_pylist(eventos)

    buffer = io.BytesIO()
    pq.write_table(
        tabela,
        buffer,
        compression="snappy",
    )

    buffer.seek(0)

    request_id = context.aws_request_id

    event_date = datetime.fromisoformat(
        eventos[0]["event_timestamp"]
    )   

    key = (
        f"{PREFIX}"
        f"ano={event_date.year}/"
        f"mes={event_date.month:02d}/"
        f"dia={event_date.day:02d}/"
        f"eventos_{request_id}.parquet"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    print(
        f"[LAMBDA] {len(eventos)} evento(s) gravado(s): "
        f"s3://{BUCKET}/{key}"
    )

    return {
        "processados": len(eventos),
        "s3_key": key,
    }