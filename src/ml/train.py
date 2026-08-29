from pathlib import Path

import joblib
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

from src.ml.dataset import carregar_dataset


RANDOM_STATE = 42
TEST_SIZE = 0.20

MODEL_DIR = Path("models")
MODEL_PATH = MODEL_DIR / "random_forest.pkl"


def main() -> None:

    print("=" * 70)
    print("TREINAMENTO - CLASSIFICAÇÃO DE RISCO")
    print("=" * 70)

    # ------------------------------------------------------------------
    # 1. CARREGAMENTO DO DATASET
    # ------------------------------------------------------------------

    df = carregar_dataset()

    # ------------------------------------------------------------------
    # 2. DEFINIÇÃO DAS FEATURES
    # ------------------------------------------------------------------

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
    # 3. PREPARAÇÃO DOS DADOS
    # ------------------------------------------------------------------

    modelagem = df[
        FEATURES_NUM + FEATURES_CAT + ["risco"]
    ].copy()

    # Preenche valores ausentes das variáveis numéricas
    # utilizando a mediana, exatamente como no notebook.
    modelagem[FEATURES_NUM] = modelagem[FEATURES_NUM].fillna(
        modelagem[FEATURES_NUM].median()
    )

    # ------------------------------------------------------------------
    # 4. TRANSFORMAÇÃO DAS VARIÁVEIS CATEGÓRICAS
    # ------------------------------------------------------------------

    X = pd.get_dummies(
        modelagem[FEATURES_NUM + FEATURES_CAT],
        columns=FEATURES_CAT,
        drop_first=True,
    )

    y = modelagem["risco"]

    print(f"[INFO] Registros para modelagem: {len(X):,}")
    print(f"[INFO] Features antes do treinamento: {X.shape[1]}")

    print("[INFO] Target:")
    print(y.value_counts().sort_index().to_string())

    # ------------------------------------------------------------------
    # 5. SEPARAÇÃO TREINO / TESTE
    # ------------------------------------------------------------------

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        stratify=y,
        random_state=RANDOM_STATE,
    )

    print()
    print(f"[INFO] Treino: {len(X_train):,} registros")
    print(f"[INFO] Teste:  {len(X_test):,} registros")

    # ------------------------------------------------------------------
    # 6. RANDOM FOREST
    # ------------------------------------------------------------------

    rf = RandomForestClassifier(
        n_estimators=300,
        max_depth=8,
        min_samples_leaf=10,
        class_weight="balanced",
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )

    # ------------------------------------------------------------------
    # 7. TREINAMENTO
    # ------------------------------------------------------------------

    print()
    print("[INFO] Treinando Random Forest...")

    rf.fit(X_train, y_train)

    print("[INFO] Treinamento concluído.")

    # ------------------------------------------------------------------
    # 8. PREVISÕES
    # ------------------------------------------------------------------

    y_pred = rf.predict(X_test)
    y_prob = rf.predict_proba(X_test)[:, 1]

    # ------------------------------------------------------------------
    # 9. MÉTRICAS
    # ------------------------------------------------------------------

    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    auc_roc = roc_auc_score(y_test, y_prob)

    print()
    print("=" * 70)
    print("RESULTADOS")
    print("=" * 70)

    print(f"Accuracy:  {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall:    {recall:.4f}")
    print(f"F1-score:  {f1:.4f}")
    print(f"AUC-ROC:   {auc_roc:.4f}")

    # ------------------------------------------------------------------
    # 10. MATRIZ DE CONFUSÃO
    # ------------------------------------------------------------------

    print()
    print("MATRIZ DE CONFUSÃO")

    cm = confusion_matrix(y_test, y_pred)

    print(cm)

    # ------------------------------------------------------------------
    # 11. CLASSIFICATION REPORT
    # ------------------------------------------------------------------

    print()
    print("CLASSIFICATION REPORT")

    print(
        classification_report(
            y_test,
            y_pred,
            target_names=["sem risco", "risco"],
        )
    )

    # ------------------------------------------------------------------
    # 12. PERSISTÊNCIA DO MODELO
    # ------------------------------------------------------------------

    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    joblib.dump(rf, MODEL_PATH)

    print()
    print(f"[INFO] Modelo salvo em: {MODEL_PATH}")

    print("=" * 70)


if __name__ == "__main__":
    main()