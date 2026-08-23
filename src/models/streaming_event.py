from dataclasses import asdict, dataclass
from datetime import datetime
import json


@dataclass(frozen=True)
class StreamingEvent:
    event_id: str
    event_type: str
    event_timestamp: str
    municipio_id: int
    indicador: str
    valor: float

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False
        )

    @classmethod
    def from_dict(cls, data: dict) -> "StreamingEvent":
        return cls(
            event_id=data["event_id"],
            event_type=data["event_type"],
            event_timestamp=data["event_timestamp"],
            municipio_id=int(data["municipio_id"]),
            indicador=data["indicador"],
            valor=float(data["valor"]),
        )

    @classmethod
    def from_json(cls, data: str) -> "StreamingEvent":
        return cls.from_dict(json.loads(data))