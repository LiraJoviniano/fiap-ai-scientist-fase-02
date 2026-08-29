from pathlib import Path

import joblib
import pandas as pd


MODEL_PATH = Path("models/random_forest.pkl")
OUTPUT_PATH = Path("models/feature_importance_random_forest.csv")


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


def reconstruir_features(df: pd.DataFrame) -> pd.DataFrame:
    """Reconstrói as mesmas features utilizadas no treinamento."""

    dados = df[
        FEATURES_NUM + FEATURES_CAT
    ].copy()

    dados[FEATURES_NUM] = dados[FEATURES_NUM].fillna(
        dados[FEATURES_NUM].median()
    )

    dados = pd.get_dummies(
        dados,
        columns=FEATURES_CAT,
        drop_first=True,
    )

    return dados


def main() -> None:

    print("=" * 70)
    print("INTERPRETABILIDADE - RANDOM FOREST")
    print("=" * 70)

    # ------------------------------------------------------------------
    # 1. Carrega o modelo
    # ------------------------------------------------------------------

    print()
    print("[INFO] Carregando modelo Random Forest...")

    rf = joblib.load(MODEL_PATH)

    # ------------------------------------------------------------------
    # 2. Carrega o dataset
    # ------------------------------------------------------------------

    from src.ml.dataset import carregar_dataset

    df = carregar_dataset()

    # ------------------------------------------------------------------
    # 3. Reconstrói as features
    # ------------------------------------------------------------------

    X = reconstruir_features(df)

    # Mantém somente as features utilizadas pelo modelo.
    # O Random Forest informa a quantidade esperada.
    n_features_modelo = len(rf.feature_importances_)

    print(
        f"[INFO] Features reconstruídas: {X.shape[1]}"
    )

    print(
        f"[INFO] Features utilizadas pelo modelo: "
        f"{n_features_modelo}"
    )

    if X.shape[1] != n_features_modelo:
        raise ValueError(
            "O número de features reconstruídas não corresponde "
            "ao número de features utilizadas pelo modelo."
        )

    # ------------------------------------------------------------------
    # 4. Importância das features
    # ------------------------------------------------------------------

    importance = pd.DataFrame(
        {
            "feature": X.columns,
            "importance": rf.feature_importances_,
        }
    )

    importance = importance.sort_values(
        "importance",
        ascending=False,
    ).reset_index(drop=True)

    # ------------------------------------------------------------------
    # 5. Exibição
    # ------------------------------------------------------------------

    print()
    print("=" * 70)
    print("TOP 10 FEATURES MAIS IMPORTANTES")
    print("=" * 70)

    print(
        importance.head(10).to_string(
            index=False,
            formatters={
                "importance": "{:.4f}".format,
            },
        )
    )

    # ------------------------------------------------------------------
    # 6. Salva resultado
    # ------------------------------------------------------------------

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    importance.to_csv(
        OUTPUT_PATH,
        index=False,
    )

    print()
    print(
        f"[INFO] Resultado salvo em: {OUTPUT_PATH}"
    )

    print("=" * 70)


if __name__ == "__main__":
    main()