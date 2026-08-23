import boto3

from src.config.settings import settings


class KinesisClient:
    def __init__(self):
        self.client = boto3.client(
            "kinesis",
            region_name=settings.AWS_REGION,
        )
        self.stream_name = settings.KINESIS_STREAM_NAME

    def put_record(self, data: str, partition_key: str) -> dict:
        response = self.client.put_record(
            StreamName=self.stream_name,
            Data=data.encode("utf-8"),
            PartitionKey=partition_key,
        )

        return response