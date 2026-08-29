from pathlib import Path

import pandas as pd

# NOTA: valores digitados manualmente a partir da última execução de
# train.py / do notebook de modelagem. Se o modelo for retreinado com
# mudança de features, dados ou hiperparâmetros, atualizar esta lista
# na mão — não é lido automaticamente do treino.
RESULTADOS = [
    {
        "Modelo": "Logistic Regression",
        "AUC-ROC CV": 0.774,
        "AUC-ROC Teste": 0.744,
    },
    {
        "Modelo": "Random Forest",
        "AUC-ROC CV": 0.766,
        "AUC-ROC Teste": 0.748,
    },
]


def main():
    df = pd.DataFrame(RESULTADOS)

    print()
    print("=" * 70)
    print("COMPARAÇÃO DOS MODELOS")
    print("=" * 70)
    print()

    print(
        df.to_string(
            index=False,
            formatters={
                "AUC-ROC CV": "{:.3f}".format,
                "AUC-ROC Teste": "{:.3f}".format,
            },
        )
    )

    melhor = df.loc[df["AUC-ROC Teste"].idxmax()]

    print()
    print("=" * 70)
    print("MODELO SELECIONADO")
    print("=" * 70)

    print(f"Modelo: {melhor['Modelo']}")
    print(f"AUC-ROC (teste): {melhor['AUC-ROC Teste']:.3f}")
    print("Critério: maior AUC-ROC no conjunto de teste")

    output = Path("models/comparacao_modelos.csv")
    output.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output, index=False)

    print()
    print(f"[INFO] Comparação salva em: {output}")


if __name__ == "__main__":
    main()