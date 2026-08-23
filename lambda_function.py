import base64
import json

import boto3

S3_BUCKET = "fiap-postech-challenge-fase2"
S3_PREFIX = "bronze/streaming/"

s3 = boto3.client("s3")


def lambda_handler(event, context):
    processados = 0

    for record in event["Records"]:
        data = base64.b64decode(
            record["kinesis"]["data"]
        ).decode("utf-8")

        evento = json.loads(data)

        event_id = evento["event_id"]

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"{S3_PREFIX}{event_id}.json",
            Body=data.encode("utf-8"),
            ContentType="application/json",
        )

        print(
            f"[LAMBDA] Evento processado: {event_id} | "
            f"Município: {evento['municipio_id']} | "
            f"Valor: {evento['valor']}"
        )

        processados += 1

    return {
        "statusCode": 200,
        "processados": processados,
    }
