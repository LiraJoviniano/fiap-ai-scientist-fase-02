"""
Ingestão do Censo Escolar — fonte externa, camada Bronze.

Extrai o recorte do dataset `br_inep_censo_escolar` no BigQuery, grava em
Parquet e envia ao S3, seguindo o mesmo caminho da ingestão principal.

O recorte é deliberado: 22 colunas de 455 e dois anos, o que reduz a
varredura de 6,17 GB para 56 MB — medido em src/estimativa_censo.py. A
Bronze permanece fiel ao que foi extraído; nenhuma transformação ocorre
aqui, e o filtro de rede fica para a Gold, porque é escolha analítica.

Uso:
    python src/ingestao_censo.py
"""

import logging
from pathlib import Path

from google.cloud import bigquery

from cloud.s3 import upload_file
from config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


TABELA = "basedosdados.br_inep_censo_escolar.escola"

DESTINO_LOCAL = Path("data/bronze/censo_escolar")
ARQUIVO = "escola.parquet"

# Mesma cobertura da avaliação de alfabetização, para o join fechar
ANOS = [2023, 2024]

# O recorte medido varre 56 MB. O teto protege contra engano na consulta:
# acima dele a query falha antes de executar, sem gerar custo.
MAX_BYTES_FATURADOS = 1 * 1024**3

# ---------------------------------------------------------------------------
# Colunas do recorte
# ---------------------------------------------------------------------------

CHAVES = [
    "ano",
    "id_escola",
    "sigla_uf",
    "id_municipio",
    "rede",
    "tipo_localizacao",    
    "tipo_situacao_funcionamento",
]

# Define o universo na Gold: só escolas que ofertam anos iniciais do
# fundamental. O indicador avalia o 2º ano.
FILTRO_ETAPA = "etapa_ensino_fundamental_anos_iniciais"

# Permitem ponderar por aluno em vez de por escola. Município com uma
# escola grande adequada e cinco pequenas precárias não deve aparecer como
# 17% adequado quando a maioria dos alunos está bem atendida.
CONTAGENS = [
    "quantidade_matricula_fundamental_anos_iniciais",
    "quantidade_matricula_fundamental_anos_iniciais_integral",
    "quantidade_docente_fundamental_anos_iniciais",
    "quantidade_turma_fundamental_anos_iniciais",
    "quantidade_matricula_zona_residencia_rural",
    "quantidade_matricula_utiliza_transporte_publico",
]

INFRAESTRUTURA = [
    "biblioteca",
    "biblioteca_sala_leitura",
    "laboratorio_informatica",
    "banda_larga",
    "agua_rede_publica",
    "agua_potavel",
    "energia_rede_publica",
    "esgoto_rede_publica",
    "esgoto_fossa_septica",
    "alimentacao",
]

COLUNAS = CHAVES + [FILTRO_ETAPA] + CONTAGENS + INFRAESTRUTURA


def montar_sql() -> str:
    """
    Monta a consulta do recorte.

    Seleção explícita de colunas e filtro de ano são o que reduz o custo:
    o BigQuery cobra por coluna varrida.
    """

    campos = ",\n        ".join(COLUNAS)
    anos = ", ".join(str(a) for a in ANOS)

    return f"""
    SELECT
        {campos}
    FROM `{TABELA}`
    WHERE ano IN ({anos})
    """


def extrair():
    """Extrai o recorte do Censo Escolar."""

    client = bigquery.Client(project=settings.GCP_PROJECT_ID)

    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=MAX_BYTES_FATURADOS
    )

    logger.info(f"Extraindo {len(COLUNAS)} colunas de 455, anos {ANOS}")

    job = client.query(montar_sql(), job_config=job_config)
    df = job.result().to_dataframe()

    varrido = job.total_bytes_processed / 1024**2

    logger.info(f"Registros: {len(df):,}")
    logger.info(f"Varredura: {varrido:.1f} MB")
    logger.info(f"Escolas com anos iniciais: "
                f"{int(df[FILTRO_ETAPA].sum()):,}")
    logger.info(f"Municipios distintos: {df['id_municipio'].nunique():,}")

    return df


def gravar(df) -> Path:
    """Grava o Parquet na Bronze local."""

    DESTINO_LOCAL.mkdir(parents=True, exist_ok=True)

    caminho = DESTINO_LOCAL / ARQUIVO
    df.to_parquet(caminho, index=False)

    tamanho = caminho.stat().st_size / 1024**2

    logger.info(f"Arquivo salvo: {caminho} ({tamanho:.1f} MB)")

    return caminho


def main():
    logger.info("=" * 60)
    logger.info("CENSO ESCOLAR — INGESTAO PARA A BRONZE")
    logger.info("=" * 60)

    df = extrair()
    caminho = gravar(df)

    upload_file(str(caminho))

    logger.info("=" * 60)
    logger.info("Proximo passo: rodar o crawler para catalogar a nova pasta")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
