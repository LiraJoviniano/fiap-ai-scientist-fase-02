from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[2]

load_dotenv(BASE_DIR / ".env")


@dataclass(frozen=True)
class Settings:
    GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "")
    GCP_DATASET: str = os.getenv("GCP_DATASET", "")

    AWS_REGION: str = os.getenv("AWS_REGION", "")
    AWS_BUCKET: str = os.getenv("AWS_BUCKET", "")

    KINESIS_STREAM_NAME: str = os.getenv(
        "KINESIS_STREAM_NAME",
        "alfabetizacao-events-dev"
    )

    PIPELINE_ENV: str = os.getenv("PIPELINE_ENV", "dev")


settings = Settings()