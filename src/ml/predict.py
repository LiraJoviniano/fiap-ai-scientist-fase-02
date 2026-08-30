from pathlib import Path

import joblib
import pandas as pd

from src.ml.dataset import carregar_dataset


# ------------------------------------------------------------------
# Caminhos
# ------------------------------------------------------------------

MODEL_PATH = Path(
    "models/random_forest.pkl"
)

KMEANS_PATH = Path(
    "models/kmeans.pkl"
)

KMEANS_SCALER_PATH = Path(
    "models/kmeans_scaler.pkl"
)

FEATURES_CLUSTER = [
    "indice_infraestrutura",
    "pct_matricula_integral",
    "pct_matricula_biblioteca",
    "pct_matricula_lab_informatica",
    "pct_matricula_banda_larga",
    "pct_matricula_esgoto_adequado",
    "pct_escolas_urbanas",
    "alunos_por_docente",
    "alunos_por_turma",
    "pct_matricula_rural",
]

OUTPUT_PARQUET = Path(
    "results/gold_ml_risco_municipio.parquet"
)

OUTPUT_CSV = Path(
    "results/gold_ml_risco_municipio.csv"
)

# Mesmas features utilizadas em train.py — mantidas em sincronia manual.
FEATURES_NUM = [
    "total_escolas",
    "total_matriculas",
    "alunos_por_docente",
    "alunos_por_turma",
    "pct_matricula_integral",
    "pct_matricula_biblioteca",
    "pct_matricula_lab_informatica",
    "pct_matricula_banda_larga",
    "pct_matricula_esgoto_adequado",
    "pct_escolas_urbanas",
    "indice_infraestrutura",
    "pct_matricula_rural",
    "pct_matricula_transporte",
    "taxa_2023",
]

FEATURES_CAT = [
    "sigla_uf",
    "regiao",
]


# ------------------------------------------------------------------
# Execução
# ------------------------------------------------------------------

def main() -> None:

    print("=" * 70)
    print("PREDIÇÃO - CLASSIFICAÇÃO DE RISCO")
    print("=" * 70)

    # ------------------------------------------------------------------
    # Carregamento do dataset (mesma base e mesmos filtros do treino:
    # elegivel_meta, sem_meta e a exclusão do RS — ver dataset.py)
    # ------------------------------------------------------------------

    print()
    print("[INFO] Carregando dataset...")

    df = carregar_dataset()

    print(f"[INFO] Registros carregados: {len(df):,}")

    # ------------------------------------------------------------------
    # Carregamento do modelo
    # ------------------------------------------------------------------

    print()
    print("[INFO] Carregando modelo Random Forest...")

    model = joblib.load(MODEL_PATH)

    print(f"[INFO] Modelo carregado: {MODEL_PATH}")

    kmeans = joblib.load(KMEANS_PATH)
    kmeans_scaler = joblib.load(KMEANS_SCALER_PATH)

    print(f"[INFO] Clustering carregado: {KMEANS_PATH}")

    # ------------------------------------------------------------------
    # Features esperadas pelo modelo
    # ------------------------------------------------------------------

    if not hasattr(model, "feature_names_in_"):
        raise ValueError(
            "O modelo não possui os nomes das features "
            "utilizadas no treinamento."
        )

    feature_names = list(model.feature_names_in_)

    print(
        f"[INFO] Features esperadas pelo modelo: "
        f"{len(feature_names)}"
    )

    # ------------------------------------------------------------------
    # Preparação das features (mesma lógica de train.py: seleciona
    # explicitamente FEATURES_NUM + FEATURES_CAT, preenche nulos com a
    # mediana e aplica one-hot com drop_first — a correção de outliers
    # já veio pronta de carregar_dataset())
    # ------------------------------------------------------------------

    modelagem = df[FEATURES_NUM + FEATURES_CAT].copy()

    modelagem[FEATURES_NUM] = modelagem[FEATURES_NUM].fillna(
        modelagem[FEATURES_NUM].median()
    )

    X = pd.get_dummies(
        modelagem,
        columns=FEATURES_CAT,
        drop_first=True,
    )

    # ------------------------------------------------------------------
    # Alinhamento com as features do modelo
    # ------------------------------------------------------------------

    # Garante exatamente as mesmas colunas
    # utilizadas durante o treinamento.
    X = X.reindex(
        columns=feature_names,
        fill_value=0,
    )

    print(
        f"[INFO] Features após preprocessing: "
        f"{X.shape[1]}"
    )

    # Verificação final
    if X.shape[1] != len(feature_names):
        raise ValueError(
            "O número de features após o preprocessing "
            "não corresponde ao esperado pelo modelo."
        )

    # ------------------------------------------------------------------
    # Predição
    # ------------------------------------------------------------------

    print()
    print("[INFO] Gerando previsões...")

    predicao = model.predict(X)

    prob_risco = model.predict_proba(X)[:, 1]

    X_cluster = df[FEATURES_CLUSTER].fillna(df[FEATURES_CLUSTER].median())
    X_cluster_scaled = kmeans_scaler.transform(X_cluster)
    cluster = kmeans.predict(X_cluster_scaled)

    print("[INFO] Previsões geradas.")

    # ------------------------------------------------------------------
    # Montagem do resultado
    # ------------------------------------------------------------------

    resultado = df[
        ["id_municipio", "sigla_uf", "regiao", "classificacao_trajetoria"]
    ].copy()

    resultado["cluster"] = cluster
    resultado["risco"] = predicao
    resultado["prob_risco"] = prob_risco

    resultado = resultado.sort_values(
        "prob_risco",
        ascending=False,
    )

    # ------------------------------------------------------------------
    # Salvamento
    # ------------------------------------------------------------------

    OUTPUT_PARQUET.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    resultado.to_parquet(
        OUTPUT_PARQUET,
        index=False,
    )

    resultado.to_csv(
        OUTPUT_CSV,
        index=False,
    )

    # ------------------------------------------------------------------
    # Resumo
    # ------------------------------------------------------------------

    print()
    print("=" * 70)
    print("RESULTADO DAS PREDIÇÕES")
    print("=" * 70)

    print()
    print("Distribuição das classificações:")

    print(
        resultado["risco"]
        .value_counts()
        .sort_index()
        .to_string()
    )

    total_risco = (
        resultado["risco"] == 1
    ).sum()

    total_sem_risco = (
        resultado["risco"] == 0
    ).sum()

    print()

    print(
        f"[INFO] Municípios classificados como risco: "
        f"{total_risco:,}"
    )

    print(
        f"[INFO] Municípios classificados como sem risco: "
        f"{total_sem_risco:,}"
    )

    # ------------------------------------------------------------------
    # Top 10
    # ------------------------------------------------------------------

    print()
    print("TOP 10 MUNICÍPIOS POR PROBABILIDADE DE RISCO")
    print("-" * 70)

    print(
        resultado.head(10).to_string(index=False)
    )

    # ------------------------------------------------------------------
    # Finalização
    # ------------------------------------------------------------------

    print()
    print(
        f"[INFO] Resultado salvo em: "
        f"{OUTPUT_PARQUET} e {OUTPUT_CSV}"
    )

    print()
    print("=" * 70)
    print("PREDIÇÃO CONCLUÍDA")
    print("=" * 70)


if __name__ == "__main__":
    main()