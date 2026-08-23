from pathlib import Path

import boto3

from src.config.settings import settings


client = boto3.client(
    "s3",
    region_name=settings.AWS_REGION
)


def upload_file(file_path: Path):

    s3_key = str(file_path).replace("\\", "/")

    if s3_key.startswith("data/"):
        s3_key = s3_key.removeprefix("data/")

    client.upload_file(
        Filename=str(file_path),
        Bucket=settings.AWS_BUCKET,
        Key=s3_key
    )

    print(f"Upload realizado: s3://{settings.AWS_BUCKET}/{s3_key}")


def upload_text(content: str, s3_key: str):

    client.put_object(
        Bucket=settings.AWS_BUCKET,
        Key=s3_key,
        Body=content.encode("utf-8"),
        ContentType="application/json"
    )

    print(f"Upload realizado: s3://{settings.AWS_BUCKET}/{s3_key}")