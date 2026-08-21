"""
Camada Gold — Glue Job (PySpark).

Lê a Silver pelo Glue Catalog e grava três datasets analíticos no S3:

    indicador_municipio    série município x ano x rede
    evolucao_temporal      série com variação ano a ano e acumulada
    trajetoria_meta_2030   uma linha por município, com ritmo até a meta
    features_municipio     uma linha por município, alvos e variáveis

A Gold agrega e não limpa: a padronização já aconteceu na Silver. O que
esta camada faz é reorganizar para consumo — dashboard, análise e
modelagem — e derivar os indicadores de negócio.

Parâmetros do Job:
    --JOB_NAME          nome do job (injetado pelo Glue)
    --BUCKET_DESTINO    bucket de escrita da Gold
    --DATABASE_SILVER   database de origem no Catalog
    --ENV               dev | prod
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ===========================================================================
# CONTRATO — decisões de agregação
# ===========================================================================

# Meta final do Compromisso Nacional Criança Alfabetizada, igual para todos
# os entes. Verificado na EDA: meta_alfabetizacao_2030 tem valor único.
META_FINAL_2030 = 80.0

ANO_BASE = 2023
ANO_REFERENCIA = 2024

# Anos restantes de 2024 até 2030, usados no cálculo do ritmo necessário
ANOS_ATE_META = 2030 - ANO_REFERENCIA

# As metas municipais são da rede Municipal
REDE_MUNICIPAL = "3"

# Convenção do INEP para localização da escola. Confirmar no dicionário do
# Censo antes de tratar como definitivo.
LOCALIZACAO_URBANA = "1"

COLUNAS_NIVEL = [f"proporcao_aluno_nivel_{i}" for i in range(9)]

TABELAS_SILVER = [
    "fato_indicador_municipio",
    "fato_escola",
    "meta_vs_resultado",
    "dim_territorio",
]

# Flags de infraestrutura agregadas por ponderação de matrícula. Município
# com uma escola grande adequada e cinco pequenas precárias não deve
# aparecer como 17% adequado quando a maioria dos alunos está bem atendida.
FLAGS_INFRAESTRUTURA = {
    "pct_matricula_biblioteca": "tem_biblioteca",
    "pct_matricula_lab_informatica": "tem_laboratorio_informatica",
    "pct_matricula_banda_larga": "tem_banda_larga",
    "pct_matricula_agua_adequada": "tem_agua_adequada",
    "pct_matricula_energia_publica": "tem_energia_publica",
    "pct_matricula_esgoto_adequado": "tem_esgoto_adequado",
    "pct_matricula_alimentacao": "tem_alimentacao",
}

# Compõem o índice sintético. Alimentação fica de fora: é quase universal e
# não discrimina municípios.
COMPOEM_INDICE = [
    "pct_matricula_biblioteca",
    "pct_matricula_lab_informatica",
    "pct_matricula_banda_larga",
    "pct_matricula_agua_adequada",
    "pct_matricula_energia_publica",
    "pct_matricula_esgoto_adequado",
]

# ---------------------------------------------------------------------------
# Schema explícito das saídas
# ---------------------------------------------------------------------------

ESQUEMA_GOLD = {
    "indicador_municipio": [
        ("ano", "int"),
        ("id_municipio", "string"),
        ("sigla_uf", "string"),
        ("regiao", "string"),
        ("rede_codigo", "string"),
        ("rede_nome", "string"),
        ("taxa_alfabetizacao", "double"),
        ("media_portugues", "double"),
        ("meta_alfabetizacao", "double"),
        ("distancia_meta", "double"),
        ("atingiu_meta", "boolean"),
        ("situacao_meta", "string"),
        ("tem_distribuicao_nivel", "boolean"),
    ] + [(c, "double") for c in COLUNAS_NIVEL],
    "evolucao_temporal": [
        ("id_municipio", "string"),
        ("sigla_uf", "string"),
        ("regiao", "string"),
        ("ano", "int"),
        ("taxa_alfabetizacao", "double"),
        ("taxa_ano_anterior", "double"),
        ("variacao_absoluta", "double"),
        ("variacao_percentual", "double"),
        ("taxa_ano_base", "double"),
        ("variacao_acumulada", "double"),
        ("meta_alfabetizacao", "double"),
        ("distancia_meta", "double"),
        ("situacao_meta", "string"),
    ],
    "trajetoria_meta_2030": [
        ("id_municipio", "string"),
        ("sigla_uf", "string"),
        ("regiao", "string"),
        ("taxa_2023", "double"),
        ("taxa_2024", "double"),
        ("variacao_anual", "double"),
        ("meta_2024", "double"),
        ("atingiu_meta_2024", "boolean"),
        ("distancia_meta_2030", "double"),
        ("ritmo_necessario", "double"),
        ("indice_trajetoria", "double"),
        ("classificacao_trajetoria", "string"),
        ("elegivel_meta", "boolean"),
    ],
    "features_municipio": [
        ("id_municipio", "string"),
        ("sigla_uf", "string"),
        ("regiao", "string"),
        # Alvos: contínuo e binário, para que quem modela escolha
        ("alvo_taxa_2024", "double"),
        ("alvo_atingiu_meta", "boolean"),
        ("elegivel_meta", "boolean"),
        # Histórico
        ("taxa_2023", "double"),
        ("variacao_anual", "double"),
        # Escala
        ("total_escolas", "int"),
        ("total_matriculas", "int"),
        # Pedagógicas
        ("alunos_por_docente", "double"),
        ("alunos_por_turma", "double"),
        ("pct_matricula_integral", "double"),
        # Infraestrutura
        ("pct_matricula_biblioteca", "double"),
        ("pct_matricula_lab_informatica", "double"),
        ("pct_matricula_banda_larga", "double"),
        ("pct_matricula_agua_adequada", "double"),
        ("pct_matricula_energia_publica", "double"),
        ("pct_matricula_esgoto_adequado", "double"),
        ("pct_matricula_alimentacao", "double"),
        ("indice_infraestrutura", "double"),
        # Contexto
        ("pct_matricula_rural", "double"),
        ("pct_matricula_transporte", "double"),
        ("pct_escolas_urbanas", "double"),
    ],
}

CLASSIFICACAO_TRAJETORIA = {
    "sem_meta": "municipio sem meta publicada, trajetoria nao avaliavel",
    "meta_atingida": "ja alcancou os 80% da meta final",
    "em_ritmo": "avanca no ritmo necessario ou acima",
    "ritmo_insuficiente": "avanca, mas abaixo do necessario para 2030",
    "retrocesso": "taxa caiu entre 2023 e 2024",
}

DESTINOS = {
    "indicador_municipio": "indicadores/indicador_municipio",
    "evolucao_temporal": "indicadores/evolucao_temporal",
    "trajetoria_meta_2030": "indicadores/trajetoria_meta_2030",
    "features_municipio": "analiticos/features_municipio",
}


# ===========================================================================
# Agregações
# ===========================================================================


def construir_indicador_municipio(
    indicador: DataFrame, integracao: DataFrame
) -> DataFrame:
    """
    Série do indicador por município, ano e rede.

    Traz a meta e o atingimento onde existem — só a rede Municipal tem meta
    no nível municipal, então as demais ficam com nulo, o que é honesto.
    """

    meta = integracao.select(
        "ano",
        "id_municipio",
        "rede_codigo",
        "meta_alfabetizacao",
        "distancia_meta",
        "atingiu_meta",
        "situacao_meta",
    )

    return indicador.join(meta, ["ano", "id_municipio", "rede_codigo"], "left")


def construir_evolucao_temporal(
    integracao: DataFrame, territorio: DataFrame
) -> DataFrame:
    """
    Série do indicador com variação ano a ano e acumulada.

    Formato longo, uma linha por município e ano — é o que um gráfico de
    linha consome, diferente da trajetória, que traz os anos em colunas.

    Com dois anos de cobertura, a variação acumulada coincide com a anual
    em 2024. As duas colunas existem porque a estrutura continua correta
    quando a fonte publicar a edição seguinte.
    """

    janela = Window.partitionBy("id_municipio").orderBy("ano")

    base = integracao.filter(F.col("rede_codigo") == REDE_MUNICIPAL).select(
        "id_municipio",
        "ano",
        "taxa_alfabetizacao",
        "meta_alfabetizacao",
        "distancia_meta",
        "situacao_meta",
    )

    resultado = (
        base.withColumn(
            "taxa_ano_anterior", F.lag("taxa_alfabetizacao").over(janela)
        )
        .withColumn(
            "taxa_ano_base",
            F.first("taxa_alfabetizacao").over(janela),
        )
        .withColumn(
            "variacao_absoluta",
            F.col("taxa_alfabetizacao") - F.col("taxa_ano_anterior"),
        )
        .withColumn(
            "variacao_acumulada",
            F.col("taxa_alfabetizacao") - F.col("taxa_ano_base"),
        )
    )

    # Divisão por zero quando a taxa anterior é nula: município que partiu
    # de zero teria variação percentual indefinida, não infinita.
    return resultado.withColumn(
        "variacao_percentual",
        F.when(
            F.col("taxa_ano_anterior") > 0,
            100
            * F.col("variacao_absoluta")
            / F.col("taxa_ano_anterior"),
        ).otherwise(F.lit(None).cast("double")),
    ).join(
        territorio.select("id_municipio", "sigla_uf", "regiao"),
        "id_municipio",
        "left",
    )


def _taxa_do_ano(integracao: DataFrame, ano: int, alias: str) -> DataFrame:
    """Extrai a taxa de um ano específico, na rede Municipal."""

    return integracao.filter(
        (F.col("ano") == ano) & (F.col("rede_codigo") == REDE_MUNICIPAL)
    ).select(
        "id_municipio",
        F.col("taxa_alfabetizacao").alias(alias),
    )


def construir_trajetoria(integracao: DataFrame, territorio: DataFrame) -> DataFrame:
    """
    Trajetória de cada município rumo à meta de 2030.

    O índice de trajetória compara o ritmo observado entre 2023 e 2024 com
    o ritmo necessário para alcançar 80% em 2030. Acima de 1 o município
    avança mais rápido do que precisa; abaixo, o ritmo atual não chega.

    Duas ressalvas que o número não carrega sozinho: dois pontos no tempo
    dão variação, não tendência; e o ritmo necessário pressupõe progresso
    linear, quando em educação os últimos pontos são os mais difíceis.
    """

    base = _taxa_do_ano(integracao, ANO_BASE, "taxa_2023")
    referencia = _taxa_do_ano(integracao, ANO_REFERENCIA, "taxa_2024")

    meta = integracao.filter(
        (F.col("ano") == ANO_REFERENCIA)
        & (F.col("rede_codigo") == REDE_MUNICIPAL)
    ).select(
        "id_municipio",
        F.col("meta_alfabetizacao").alias("meta_2024"),
        F.col("atingiu_meta").alias("atingiu_meta_2024"),
        F.col("situacao_meta"),
    )

    juncao = (
        referencia.join(base, "id_municipio", "outer")
        .join(meta, "id_municipio", "left")
        .join(
            territorio.select("id_municipio", "sigla_uf", "regiao"),
            "id_municipio",
            "left",
        )
    )

    resultado = (
        juncao.withColumn(
            "variacao_anual", F.col("taxa_2024") - F.col("taxa_2023")
        )
        .withColumn(
            "distancia_meta_2030",
            F.lit(META_FINAL_2030) - F.col("taxa_2024"),
        )
        .withColumn(
            "ritmo_necessario",
            F.col("distancia_meta_2030") / F.lit(ANOS_ATE_META),
        )
        .withColumn(
            "elegivel_meta", F.col("situacao_meta") == "comparavel"
        )
    )

    # Divisão por zero quando o município já atingiu os 80%: o índice fica
    # indefinido, e a classificação trata esse caso à parte.
    resultado = resultado.withColumn(
        "indice_trajetoria",
        F.when(
            F.col("ritmo_necessario") > 0,
            F.col("variacao_anual") / F.col("ritmo_necessario"),
        ).otherwise(F.lit(None).cast("double")),
    )

    # A ordem importa. `meta_atingida` vem primeiro porque os 80% são o
    # alvo nacional para todos: ter alcançado é fato objetivo, mesmo sem
    # meta intermediária publicada. `sem_meta` cobre apenas os casos em que
    # a trajetória não pode ser avaliada.
    classificacao = (
        F.when(F.col("taxa_2024") >= META_FINAL_2030, F.lit("meta_atingida"))
        .when(~F.col("elegivel_meta"), F.lit("sem_meta"))
        .when(F.col("variacao_anual") < 0, F.lit("retrocesso"))
        .when(F.col("indice_trajetoria") >= 1, F.lit("em_ritmo"))
        .otherwise(F.lit("ritmo_insuficiente"))
    )

    return resultado.withColumn("classificacao_trajetoria", classificacao)


def _pct_ponderado(coluna_flag: str) -> "F.Column":
    """
    Percentual ponderado por matrícula.

    Soma as matrículas das escolas que atendem à condição e divide pelo
    total — diferente de contar escolas, que trataria uma escola de 20
    alunos como equivalente a uma de 800.
    """

    return (
        100
        * F.sum(
            F.when(F.col(coluna_flag), F.col("matriculas_anos_iniciais"))
            .otherwise(0)
        )
        / F.sum("matriculas_anos_iniciais")
    )


def agregar_censo(escola: DataFrame) -> DataFrame:
    """
    Agrega o Censo Escolar da escola para o município.

    Filtra escolas em atividade que ofertam anos iniciais, na rede
    Municipal — as três condições delimitam o universo pertinente ao
    indicador de alfabetização do 2º ano.
    """

    universo = escola.filter(
        (F.col("ano") == ANO_REFERENCIA)
        & F.col("em_atividade")
        & F.col("oferta_anos_iniciais")
        & (F.col("rede_codigo") == REDE_MUNICIPAL)
        & (F.col("matriculas_anos_iniciais") > 0)
    )

    agregacoes = [
        F.count("*").cast("int").alias("total_escolas"),
        F.sum("matriculas_anos_iniciais").cast("int").alias("total_matriculas"),
        (
            F.sum("matriculas_anos_iniciais") / F.sum("docentes_anos_iniciais")
        ).alias("alunos_por_docente"),
        (
            F.sum("matriculas_anos_iniciais") / F.sum("turmas_anos_iniciais")
        ).alias("alunos_por_turma"),
        (
            100 * F.sum("matriculas_integral") / F.sum("matriculas_anos_iniciais")
        ).alias("pct_matricula_integral"),
        (
            100 * F.sum("matriculas_zona_rural") / F.sum("matriculas_anos_iniciais")
        ).alias("pct_matricula_rural"),
        (
            100 * F.sum("matriculas_transporte") / F.sum("matriculas_anos_iniciais")
        ).alias("pct_matricula_transporte"),
        (
            100
            * F.sum(
                F.when(
                    F.col("tipo_localizacao").cast("string") == LOCALIZACAO_URBANA, 1
                ).otherwise(0)
            )
            / F.count("*")
        ).alias("pct_escolas_urbanas"),
    ]

    agregacoes += [
        _pct_ponderado(flag).alias(nome)
        for nome, flag in FLAGS_INFRAESTRUTURA.items()
    ]

    agregado = universo.groupBy("id_municipio").agg(*agregacoes)

    # Índice sintético: média simples das dimensões de infraestrutura que
    # discriminam entre municípios.
    soma = F.lit(0.0)
    for coluna in COMPOEM_INDICE:
        soma = soma + F.coalesce(F.col(coluna), F.lit(0.0))

    return agregado.withColumn(
        "indice_infraestrutura", soma / F.lit(len(COMPOEM_INDICE))
    )


def construir_features(
    trajetoria: DataFrame, censo: DataFrame
) -> DataFrame:
    """
    Tabela de features no grão município, com os dois alvos.

    `elegivel_meta` distingue os municípios que podem entrar num modelo de
    risco de não atingimento daqueles que só servem para predição de taxa —
    sem meta publicada, não há classe a prever.
    """

    base = trajetoria.select(
        "id_municipio",
        "sigla_uf",
        "regiao",
        F.col("taxa_2024").alias("alvo_taxa_2024"),
        F.col("atingiu_meta_2024").alias("alvo_atingiu_meta"),
        "elegivel_meta",
        "taxa_2023",
        "variacao_anual",
    )

    return base.join(censo, "id_municipio", "left")


def aplicar_esquema(df: DataFrame, nome: str) -> DataFrame:
    """Projeta o DataFrame no schema declarado da Gold."""

    projecao = []

    for coluna, tipo in ESQUEMA_GOLD[nome]:
        if coluna in df.columns:
            projecao.append(F.col(coluna).cast(tipo).alias(coluna))
        else:
            projecao.append(F.lit(None).cast(tipo).alias(coluna))

    return df.select(*projecao)


# ===========================================================================
# Orquestração
# ===========================================================================


def construir_gold(silver: dict[str, DataFrame]) -> dict:
    """Executa a camada Gold e devolve as tabelas resultantes."""

    indicador = construir_indicador_municipio(
        silver["fato_indicador_municipio"], silver["meta_vs_resultado"]
    )

    evolucao = construir_evolucao_temporal(
        silver["meta_vs_resultado"], silver["dim_territorio"]
    )

    trajetoria = construir_trajetoria(
        silver["meta_vs_resultado"], silver["dim_territorio"]
    )

    censo = agregar_censo(silver["fato_escola"])

    features = construir_features(trajetoria, censo)

    return {
        "indicador_municipio": aplicar_esquema(indicador, "indicador_municipio"),
        "evolucao_temporal": aplicar_esquema(evolucao, "evolucao_temporal"),
        "trajetoria_meta_2030": aplicar_esquema(
            trajetoria, "trajetoria_meta_2030"
        ),
        "features_municipio": aplicar_esquema(features, "features_municipio"),
    }


def gravar(tabelas: dict, base: str, logger) -> None:
    """Grava a Gold em Parquet."""

    for nome, caminho in DESTINOS.items():
        destino = f"{base}/gold/{caminho}/"

        tabelas[nome].coalesce(1).write.mode("overwrite").parquet(destino)

        logger(f"Gravado: {destino}")


def relatar(tabelas: dict, logger) -> None:
    """Registra volumes e a composição da trajetória."""

    logger("-" * 60)

    for nome in DESTINOS:
        logger(f"{nome:24} {tabelas[nome].count():>10,} linhas")

    logger("-" * 60)
    logger("Classificacao da trajetoria ate 2030:")

    composicao = (
        tabelas["trajetoria_meta_2030"]
        .groupBy("classificacao_trajetoria")
        .count()
        .orderBy(F.col("count").desc())
        .collect()
    )

    for linha in composicao:
        logger(
            f"  {linha['classificacao_trajetoria']:22} "
            f"{linha['count']:>7,} municipios"
        )

    elegiveis = tabelas["trajetoria_meta_2030"].filter(F.col("elegivel_meta"))
    total = elegiveis.count()

    if total:
        em_ritmo = elegiveis.filter(
            F.col("classificacao_trajetoria").isin("em_ritmo", "meta_atingida")
        ).count()

        logger("-" * 60)
        logger(
            f"Em ritmo de alcancar 2030: {em_ritmo:,} de {total:,} "
            f"({em_ritmo / total * 100:.1f}%)"
        )


def ler_silver(glue_context, database: str) -> dict:
    """Lê as tabelas da Silver pelo Catalog."""

    return {
        tabela: glue_context.create_dynamic_frame.from_catalog(
            database=database, table_name=tabela
        ).toDF()
        for tabela in TABELAS_SILVER
    }


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "BUCKET_DESTINO", "DATABASE_SILVER", "ENV"],
    )

    contexto = GlueContext(SparkContext.getOrCreate())

    job = Job(contexto)
    job.init(args["JOB_NAME"], args)

    logger = contexto.get_logger().info

    logger("=" * 60)
    logger(f"CAMADA GOLD — ambiente {args['ENV']}")
    logger("=" * 60)

    silver = ler_silver(contexto, args["DATABASE_SILVER"])

    for nome, df in silver.items():
        logger(f"Silver: {nome:26} {df.count():>10,} linhas")

    tabelas = construir_gold(silver)

    gravar(tabelas, f"s3://{args['BUCKET_DESTINO']}", logger)
    relatar(tabelas, logger)

    logger("GOLD CONCLUIDA")

    job.commit()


if __name__ == "__main__":
    main()
