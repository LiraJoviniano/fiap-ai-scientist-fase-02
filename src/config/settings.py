import os
from dataclasses import dataclass
from pathlib import Path

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

    FINOPS_RELATORIO_PREFIX: str = os.getenv("FINOPS_RELATORIO_PREFIX", "")
    FINOPS_LIFECYCLE_PREFIX: str = os.getenv("FINOPS_LIFECYCLE_PREFIX", "bronze/")
    FINOPS_TRANSICAO_IA_DIAS: int = int(
        os.getenv("FINOPS_TRANSICAO_IA_DIAS", "30")
    )
    FINOPS_TRANSICAO_GLACIER_DIAS: int = int(
        os.getenv("FINOPS_TRANSICAO_GLACIER_DIAS", "90")
    )
    FINOPS_GLUE_DPU_HORAS_MES: float = float(
        os.getenv("FINOPS_GLUE_DPU_HORAS_MES", "0")
    )
    FINOPS_ATHENA_GB_MES: float = float(os.getenv("FINOPS_ATHENA_GB_MES", "0"))
    FINOPS_KINESIS_SHARD_HORAS_MES: float = float(
        os.getenv("FINOPS_KINESIS_SHARD_HORAS_MES", "0")
    )
    FINOPS_KINESIS_PUT_MILHAO_MES: float = float(
        os.getenv("FINOPS_KINESIS_PUT_MILHAO_MES", "0")
    )
    FINOPS_LAMBDA_REQUISICOES_MILHAO_MES: float = float(
        os.getenv("FINOPS_LAMBDA_REQUISICOES_MILHAO_MES", "0")
    )
    FINOPS_LAMBDA_GB_SEGUNDOS_MES: float = float(
        os.getenv("FINOPS_LAMBDA_GB_SEGUNDOS_MES", "0")
    )
    FINOPS_BIGQUERY_TIB_MES: float = float(
        os.getenv("FINOPS_BIGQUERY_TIB_MES", "0")
    )


settings = Settings()