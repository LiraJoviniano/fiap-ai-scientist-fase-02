"""
Estimativa do recorte do Censo Escolar.

Mede o volume que a ingestão da Gold varreria, sem executar a consulta e
sem gerar custo. Compara três cenários para tornar visível o efeito da
seleção de colunas e do filtro de ano.

A tabela `escola` tem 455 colunas e 6,17 GB somando todas as edições. A
Gold precisa de cerca de 25 colunas e dois anos — o BigQuery cobra por
coluna varrida, e `LIMIT` corta o retorno, não a varredura.

Uso:
    python src/estimativa_censo.py
"""

import logging

from google.cloud import bigquery

from config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


TABELA = "basedosdados.br_inep_censo_escolar.escola"

ANOS = [2023, 2024]

# Rede Municipal, para casar com o grão da integração meta x resultado
REDE_MUNICIPAL = "3"

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

# Define o universo: só escolas que ofertam anos iniciais do fundamental.
# O indicador avalia o 2º ano — escola exclusiva de EJA ou de médio não
# tem relação com alfabetização e só adicionaria ruído.
FILTRO_ETAPA = "etapa_ensino_fundamental_anos_iniciais"

# Contagens que permitem ponderar por aluno em vez de por escola. Município
# com uma escola grande adequada e cinco pequenas precárias não deve
# aparecer como 17% adequado quando a maioria dos alunos está bem atendida.
CONTAGENS = [
    "quantidade_matricula_fundamental_anos_iniciais",
    "quantidade_matricula_fundamental_anos_iniciais_integral",
    "quantidade_docente_fundamental_anos_iniciais",
    "quantidade_turma_fundamental_anos_iniciais",
    "quantidade_matricula_zona_residencia_rural",
    "quantidade_matricula_utiliza_transporte_publico",
]

# Flags de infraestrutura, agregadas depois com ponderação por matrícula
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

RECORTE = CHAVES + [FILTRO_ETAPA] + CONTAGENS + INFRAESTRUTURA

client = bigquery.Client(project=settings.GCP_PROJECT_ID)


def estimar(sql: str) -> int:
    """Retorna os bytes que a consulta varreria, sem executá-la."""

    config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    return client.query(sql, job_config=config).total_bytes_processed


def formatar(bytes_: int) -> str:
    gb = bytes_ / 1024**3
    return f"{gb:>8.3f} GB" if gb >= 0.01 else f"{bytes_ / 1024**2:>8.1f} MB"


def main():
    anos = ", ".join(str(a) for a in ANOS)
    colunas = ",\n    ".join(RECORTE)

    cenarios = {
        "tudo — todas as colunas, todos os anos": f"""
            SELECT * FROM `{TABELA}`
        """,
        "todas as colunas, filtro de ano": f"""
            SELECT * FROM `{TABELA}` WHERE ano IN ({anos})
        """,
        "recorte da Gold — colunas e ano": f"""
            SELECT
                {colunas}
            FROM `{TABELA}`
            WHERE ano IN ({anos})
        """,
        "recorte da Gold + rede Municipal": f"""
            SELECT
                {colunas}
            FROM `{TABELA}`
            WHERE ano IN ({anos})
              AND rede = '{REDE_MUNICIPAL}'
        """,
    }

    logger.info("=" * 70)
    logger.info("CENSO ESCOLAR — ESTIMATIVA DO RECORTE (dry run, sem custo)")
    logger.info("=" * 70)
    logger.info(f"Colunas do recorte: {len(RECORTE)} de 455")
    logger.info(f"Anos: {anos}")
    logger.info("")

    referencia = None

    for descricao, sql in cenarios.items():
        try:
            varrido = estimar(sql)
        except Exception as erro:
            logger.error(f"{descricao}: {erro}")
            continue

        if referencia is None:
            referencia = varrido
            reducao = ""
        else:
            reducao = f"  ({100 * (1 - varrido / referencia):.1f}% menos)"

        logger.info(f"{descricao:44} {formatar(varrido)}{reducao}")

    logger.info("")
    logger.info("-" * 70)
    logger.info("O filtro de rede nao reduz a varredura: o BigQuery cobra por")
    logger.info("coluna lida, e a coluna `rede` precisa ser lida para filtrar.")
    logger.info("Quem reduz de verdade e a selecao de colunas e a particao de ano.")


if __name__ == "__main__":
    main()
