import time

from src.cloud.kinesis import KinesisClient
from src.cloud.s3 import upload_text
from src.models.streaming_event import StreamingEvent


def main():
    kinesis = KinesisClient()

    shards = kinesis.client.list_shards(
        StreamName=kinesis.stream_name
    )["Shards"]

    if not shards:
        print("[CONSUMER] Nenhum shard encontrado.")
        return

    print(f"[CONSUMER] Stream: {kinesis.stream_name}")
    print(f"[CONSUMER] Shards encontrados: {len(shards)}")

    for shard in shards:
        shard_id = shard["ShardId"]

        iterator = kinesis.client.get_shard_iterator(
            StreamName=kinesis.stream_name,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        print(f"[CONSUMER] Lendo {shard_id}...")

        while True:
            response = kinesis.client.get_records(
                ShardIterator=iterator,
                Limit=100,
            )

            iterator = response["NextShardIterator"]

            for record in response["Records"]:
                evento = StreamingEvent.from_json(
                    record["Data"].decode("utf-8")
                )

                print(
                    f"[CONSUMER] Evento recebido: {evento.event_id} | "
                    f"Município: {evento.municipio_id} | "
                    f"Indicador: {evento.indicador} | "
                    f"Valor: {evento.valor}"
                )

                s3_key = f"bronze/streaming/{evento.event_id}.json"

                upload_text(
                    evento.to_json(),
                    s3_key
                )

            if response["Records"]:
                print(
                    f"[CONSUMER] {len(response['Records'])} "
                    "evento(s) processado(s)."
                )

            time.sleep(1)


if __name__ == "__main__":
    main()