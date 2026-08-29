from pathlib import Path

import joblib
import pandas as pd


# ------------------------------------------------------------------
# Caminhos
# ------------------------------------------------------------------

DATASET_PATH = Path(
    "data/model/gold/analiticos/features_municipio"
)

TRAJETORIA_PATH = Path(
    "data/model/gold/indicadores/trajetoria_meta_2030"
)

MODEL_PATH = Path(
    "models/random_forest.pkl"
)

OUTPUT_PATH = Path(
    "results/gold_ml_risco_municipio.parquet"
)


# ------------------------------------------------------------------
# Execução
# ------------------------------------------------------------------

def main() -> None:

    print("=" * 70)
    print("PREDIÇÃO - CLASSIFICAÇÃO DE RISCO")
    print("=" * 70)

    # ------------------------------------------------------------------
    # Carregamento do dataset
    # ------------------------------------------------------------------

    print()
    print(f"[INFO] Carregando dataset: {DATASET_PATH}")

    df = pd.read_parquet(DATASET_PATH)

    print(f"[INFO] Registros carregados: {len(df):,}")

    # ------------------------------------------------------------------
    # Join com a trajetória (necessário para classificacao_trajetoria)
    # ------------------------------------------------------------------

    print(f"[INFO] Carregando trajetória: {TRAJETORIA_PATH}")

    df_trajetoria = pd.read_parquet(TRAJETORIA_PATH)

    df = df.merge(
        df_trajetoria[["id_municipio", "classificacao_trajetoria"]],
        on="id_municipio",
        how="left",
    )

    # ------------------------------------------------------------------
    # Carregamento do modelo
    # ------------------------------------------------------------------

    print()
    print("[INFO] Carregando modelo Random Forest...")

    model = joblib.load(MODEL_PATH)

    print(f"[INFO] Modelo carregado: {MODEL_PATH}")

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
    # Preparação das features
    # ------------------------------------------------------------------

    # Remove o target e a coluna de trajetória (usada só como referência,
    # não entra no modelo) caso existam na base
    X = df.drop(
        columns=["risco", "alvo_atingiu_meta", "classificacao_trajetoria"],
        errors="ignore",
    ).copy()

    # ------------------------------------------------------------------
    # Correção de outliers de qualidade de dados (igual ao treino)
    # ------------------------------------------------------------------

    for col in ["pct_matricula_rural", "pct_matricula_transporte"]:
        if col in X.columns:
            X[col] = X[col].clip(upper=100)

    for col in ["alunos_por_docente", "alunos_por_turma"]:
        if col in X.columns:
            p1, p99 = X[col].quantile([0.01, 0.99])
            X[col] = X[col].clip(lower=p1, upper=p99)

    # ------------------------------------------------------------------
    # One-Hot Encoding
    # ------------------------------------------------------------------

    # As mesmas variáveis categóricas utilizadas no treinamento
    colunas_categoricas = [
        coluna
        for coluna in ["sigla_uf", "regiao"]
        if coluna in X.columns
    ]

    if colunas_categoricas:
        X = pd.get_dummies(
            X,
            columns=colunas_categoricas,
            dtype=int,
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

    print("[INFO] Previsões geradas.")

    # ------------------------------------------------------------------
    # Montagem do resultado
    # ------------------------------------------------------------------

    # NOTA: "cluster" não entra aqui — o K-Means do notebook ainda não
    # foi persistido (não existe um models/kmeans.pkl). Adicionar de
    # volta quando o cluster for salvo no treino.
    colunas_identificacao = [
        "id_municipio",
        "sigla_uf",
        "regiao",
        "classificacao_trajetoria",
        "elegivel_meta",
    ]

    colunas_identificacao = [
        coluna
        for coluna in colunas_identificacao
        if coluna in df.columns
    ]

    resultado = df[colunas_identificacao].copy()

    resultado["risco"] = predicao
    resultado["prob_risco"] = prob_risco

    resultado = resultado.sort_values(
        "prob_risco",
        ascending=False,
    )

    # ------------------------------------------------------------------
    # Salvamento
    # ------------------------------------------------------------------

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    resultado.to_parquet(
        OUTPUT_PATH,
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
        f"{OUTPUT_PATH}"
    )

    print()
    print("=" * 70)
    print("PREDIÇÃO CONCLUÍDA")
    print("=" * 70)


if __name__ == "__main__":
    main()