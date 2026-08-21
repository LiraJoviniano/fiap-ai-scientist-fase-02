"""
Exploração do Censo Escolar antes da extração.

Lista as tabelas do dataset, o volume de cada uma e as colunas
disponíveis, sem executar nenhuma consulta paga. É o passo anterior a
qualquer ingestão: presumir schema foi o que mais custou tempo na Silver.

A tabela `matricula` tem dezenas de GB e não deve ser extraída inteira —
o volume aparece aqui justamente para tornar isso visível.

Uso:
    python src/explorar_censo.py
    python src/explorar_censo.py escola     # detalha uma tabela
"""

import logging
import sys

from google.cloud import bigquery

from config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


DATASET = "basedosdados.br_inep_censo_escolar"

# Acima deste volume, extrair a tabela inteira é inviável no orçamento e
# na cota diária configurada no projeto.
LIMITE_GB_SEGURO = 5

# Colunas de interesse para o grão município. A busca é por substring:
# o objetivo é descobrir o que existe, não presumir nomes.
TERMOS_INTERESSE = [
    "municipio",
    "uf",
    "ano",
    "rede",
    "dependencia",
    "biblioteca",
    "internet",
    "energia",
    "agua",
    "esgoto",
    "laboratorio",
    "quadra",
    "computador",
    "localizacao",
    "docente",
    "turma",
    "matricula",
]

client = bigquery.Client(project=settings.GCP_PROJECT_ID)


def listar_tabelas() -> list[str]:
    """Lista as tabelas do dataset."""

    return sorted(t.table_id for t in client.list_tables(DATASET))


def estimar(tabela: str) -> dict:
    """Mede volume e colunas sem executar consulta."""

    meta = client.get_table(f"{DATASET}.{tabela}")

    sql = f"SELECT * FROM `{DATASET}.{tabela}`"

    config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=config)

    return {
        "tabela": tabela,
        "linhas": meta.num_rows,
        "colunas": len(meta.schema),
        "gb": job.total_bytes_processed / 1024**3,
        "campos": [c.name for c in meta.schema],
    }


def visao_geral() -> list[dict]:
    """Panorama do dataset, com alerta para tabelas grandes."""

    logger.info("=" * 70)
    logger.info("CENSO ESCOLAR — VISAO GERAL")
    logger.info("=" * 70)

    resultados = []

    for tabela in listar_tabelas():
        try:
            info = estimar(tabela)
        except Exception as erro:
            logger.error(f"{tabela}: {erro}")
            continue

        alerta = "  <-- GRANDE DEMAIS" if info["gb"] > LIMITE_GB_SEGURO else ""

        logger.info(
            f"{info['tabela']:24} {info['linhas']:>14,} linhas  "
            f"{info['colunas']:>3} colunas  {info['gb']:>9.2f} GB{alerta}"
        )

        resultados.append(info)

    logger.info("-" * 70)
    logger.info(
        "Tabelas acima de "
        f"{LIMITE_GB_SEGURO} GB nao devem ser extraidas inteiras."
    )

    return resultados


def detalhar(tabela: str) -> None:
    """Mostra as colunas de uma tabela, destacando as de interesse."""

    info = estimar(tabela)

    logger.info("=" * 70)
    logger.info(f"{tabela.upper()} — {info['linhas']:,} linhas, "
                f"{info['colunas']} colunas, {info['gb']:.2f} GB")
    logger.info("=" * 70)

    relevantes = [
        c for c in info["campos"]
        if any(termo in c.lower() for termo in TERMOS_INTERESSE)
    ]

    logger.info(f"\nColunas de interesse ({len(relevantes)}):\n")
    for coluna in relevantes:
        logger.info(f"  {coluna}")

    outras = [c for c in info["campos"] if c not in relevantes]

    logger.info(f"\nDemais colunas ({len(outras)}):\n")
    for i in range(0, len(outras), 4):
        logger.info("  " + "  ".join(f"{c:32}" for c in outras[i:i + 4]))

    # Selecionar colunas e filtrar o ano reduz o custo de verdade:
    # o BigQuery cobra por coluna varrida, e LIMIT nao reduz a varredura.
    logger.info("")
    logger.info("Para estimar o custo de um recorte, use dry run com as")
    logger.info("colunas e o filtro de ano que a Gold realmente precisa.")


def main():
    if len(sys.argv) > 1:
        detalhar(sys.argv[1])
        return

    visao_geral()

    logger.info("")
    logger.info("Para detalhar uma tabela:")
    logger.info("    python src/explorar_censo.py escola")


if __name__ == "__main__":
    main()
