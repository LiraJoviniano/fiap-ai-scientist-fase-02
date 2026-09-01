from pathlib import Path

import pandas as pd


DATA_DIR = Path("data/model/gold")

FEATURES_PATH = DATA_DIR / "analiticos" / "features_municipio"
TRAJETORIA_PATH = DATA_DIR / "indicadores" / "trajetoria_meta_2030"


def carregar_dataset() -> pd.DataFrame:
    """Carrega e prepara o dataset para modelagem."""

    # --------------------------------------------------------------
    # Leitura das tabelas Gold
    # --------------------------------------------------------------

    df_features = pd.read_parquet(FEATURES_PATH)
    df_trajetoria = pd.read_parquet(TRAJETORIA_PATH)

    # --------------------------------------------------------------
    # Join entre features e trajetória
    # --------------------------------------------------------------

    df = df_features.merge(
        df_trajetoria[
            ["id_municipio", "classificacao_trajetoria"]
        ],
        on="id_municipio",
        how="inner",
    )

    # --------------------------------------------------------------
    # Filtros definidos no notebook
    # --------------------------------------------------------------

    df = df[df["elegivel_meta"] == True]

    df = df[
        df["classificacao_trajetoria"] != "sem_meta"
    ]

    df = df.dropna(
        subset=["classificacao_trajetoria"]
    )

    # Exclui o RS da base de modelagem inteira: a queda do indicador
    # entre 2023-2024 no estado tem assinatura de mudança metodológica
    # na fonte (ver README, seção 14), atingindo 89,6% dos municípios
    # do RS — não é retrocesso educacional genuíno. Mesmo filtro
    # aplicado no notebook (célula 16).
    df = df[df["sigla_uf"] != "RS"]  

    # --------------------------------------------------------------
    # Target de classificação
    #
    # risco = 1:
    #   retrocesso
    #   ritmo_insuficiente
    #
    # risco = 0:
    #   demais classificações
    # --------------------------------------------------------------

    df["risco"] = (
        df["classificacao_trajetoria"]
        .isin(["retrocesso", "ritmo_insuficiente"])
        .astype(int)
    )


    # Correção de outliers de qualidade de dados

    for col in ["pct_matricula_rural", "pct_matricula_transporte"]:
        df[col] = df[col].clip(upper=100)

    for col in ["alunos_por_docente", "alunos_por_turma"]:
        p1, p99 = df[col].quantile([0.01, 0.99])
        df[col] = df[col].clip(lower=p1, upper=p99)
    return df


# def preparar_dataset(
#     df: pd.DataFrame,
# ) -> tuple[pd.DataFrame, pd.Series]:
#     """Separa features e variável alvo para classificação."""

#     # Features utilizadas pelo modelo
#     features = [
#         "total_escolas",
#         "total_matriculas",
#         "alunos_por_docente",
#         "alunos_por_turma",
#         "pct_matricula_integral",
#         "pct_matricula_biblioteca",
#         "pct_matricula_lab_informatica",
#         "pct_matricula_banda_larga",
#         "pct_matricula_agua_adequada",
#         "pct_matricula_energia_publica",
#         "pct_matricula_esgoto_adequado",
#         "pct_matricula_alimentacao",
#         "indice_infraestrutura",
#         "pct_matricula_rural",
#         "pct_matricula_transporte",
#         "pct_escolas_urbanas",
#         "taxa_2023",
#         "sigla_uf",
#         "regiao",
#     ]

#     X = df[features].copy()

#     y = df["risco"].copy()

#     return X, y