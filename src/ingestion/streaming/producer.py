from src.cloud.kinesis import KinesisClient
from src.models.streaming_event import StreamingEvent


def main():
    kinesis = KinesisClient()

    eventos = [
        StreamingEvent(
            event_id="evt-000001",
            event_type="atualizacao_indicador",
            event_timestamp="2026-08-18T19:10:00-03:00",
            municipio_id=3550308,
            indicador="crianca_alfabetizada",
            valor=0.83,
        ),
        StreamingEvent(
            event_id="evt-000002",
            event_type="atualizacao_indicador",
            event_timestamp="2026-08-18T19:11:00-03:00",
            municipio_id=3304557,
            indicador="crianca_alfabetizada",
            valor=0.79,
        ),
        StreamingEvent(
            event_id="evt-000003",
            event_type="atualizacao_indicador",
            event_timestamp="2026-08-18T19:12:00-03:00",
            municipio_id=3550308,
            indicador="crianca_alfabetizada",
            valor=0.84,
        ),
    ]

    for evento in eventos:
        partition_key = f"municipio-{evento.municipio_id}"

        resposta = kinesis.put_record(
            data=evento.to_json(),
            partition_key=partition_key,
        )

        print(
            f"[PRODUCER] Evento enviado: {evento.event_id} | "
            f"Município: {evento.municipio_id} | "
            f"Shard: {resposta['ShardId']}"
        )


if __name__ == "__main__":
    main()