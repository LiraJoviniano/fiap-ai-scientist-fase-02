from pathlib import Path

import pandas as pd


# ------------------------------------------------------------------
# Caminhos
# ------------------------------------------------------------------

INPUT_PATH = Path(
    "results/gold_ml_risco_municipio.parquet"
)

OUTPUT_PATH = Path(
    "results/analise_risco_uf.csv"
)

# UFs excluídas da agregação por apresentar artefato de dado conhecido
# (ver README, seção 14: queda uniforme de ~20 p.p. no RS entre
# 2023-2024, incompatível com aprendizado real, tratada como mudança
# metodológica na fonte). Incluir o RS aqui infla artificialmente o
# risco aparente do estado.
UFS_EXCLUIDAS_DA_ANALISE = ["RS"]


def main() -> None:

    print("=" * 70)
    print("ANÁLISE DE RISCO POR UF (PREVISTO PELO MODELO)")
    print("=" * 70)
    print(
        "\n[ATENÇÃO] Os percentuais abaixo refletem o RISCO PREVISTO "
        "pelo Random Forest, não a trajetória observada. Podem "
        "divergir dos percentuais de 'trajetória observada' "
        "reportados no notebook/README (ex.: BA=82,0% observado)."
    )

    # ------------------------------------------------------------------
    # 1. Carregamento
    # ------------------------------------------------------------------

    print()
    print(f"[INFO] Carregando resultado: {INPUT_PATH}")

    df = pd.read_parquet(INPUT_PATH)

    print(
        f"[INFO] Municípios analisados: {len(df):,}"
    )

    # ------------------------------------------------------------------
    # 2. Exclusão de UFs com artefato de dado conhecido
    # ------------------------------------------------------------------

    if UFS_EXCLUIDAS_DA_ANALISE:
        print(
            f"[INFO] Excluindo UFs da análise (artefato de dado "
            f"conhecido, ver README seção 14): "
            f"{', '.join(UFS_EXCLUIDAS_DA_ANALISE)}"
        )
        df = df[~df["sigla_uf"].isin(UFS_EXCLUIDAS_DA_ANALISE)]

    # Restringe à mesma população usada no treino/notebook (municípios
    # elegíveis e com meta aplicável). Sem esse filtro, o percentual
    # de risco por UF conta municípios sem trajetória real definida,
    # divergindo do número já reportado no README/notebook.
    if "elegivel_meta" in df.columns:
        df = df[df["elegivel_meta"] == True]
    if "classificacao_trajetoria" in df.columns:
        df = df[df["classificacao_trajetoria"] != "sem_meta"]

    # ------------------------------------------------------------------
    # 3. Agregação por UF
    # ------------------------------------------------------------------

    analise = (
        df.groupby("sigla_uf")
        .agg(
            municipios=("sigla_uf", "size"),
            municipios_em_risco=("risco", "sum"),
            probabilidade_media=("prob_risco", "mean"),
            maior_probabilidade=("prob_risco", "max"),
        )
        .reset_index()
    )

    # ------------------------------------------------------------------
    # 4. Métricas derivadas
    # ------------------------------------------------------------------

    analise["municipios_sem_risco"] = (
        analise["municipios"]
        - analise["municipios_em_risco"]
    )

    analise["percentual_em_risco"] = (
        analise["municipios_em_risco"]
        / analise["municipios"]
        * 100
    )

    # Ordena pela proporção de municípios em risco
    analise = analise.sort_values(
        "percentual_em_risco",
        ascending=False,
    ).reset_index(drop=True)

    # ------------------------------------------------------------------
    # 5. Organização das colunas
    # ------------------------------------------------------------------

    analise = analise[
        [
            "sigla_uf",
            "municipios",
            "municipios_em_risco",
            "municipios_sem_risco",
            "percentual_em_risco",
            "probabilidade_media",
            "maior_probabilidade",
        ]
    ]

    # ------------------------------------------------------------------
    # 6. Salvamento
    # ------------------------------------------------------------------

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    analise.to_csv(
        OUTPUT_PATH,
        index=False,
    )

    # ------------------------------------------------------------------
    # 7. Exibição
    # ------------------------------------------------------------------

    print()
    print("=" * 70)
    print("RISCO POR UF")
    if UFS_EXCLUIDAS_DA_ANALISE:
        print(
            f"(excluídas: {', '.join(UFS_EXCLUIDAS_DA_ANALISE)} "
            f"— ver README seção 14)"
        )
    print("=" * 70)
    print()

    exibicao = analise.copy()

    exibicao["percentual_em_risco"] = (
        exibicao["percentual_em_risco"]
        .map(lambda x: f"{x:.1f}%")
    )

    exibicao["probabilidade_media"] = (
        exibicao["probabilidade_media"]
        .map(lambda x: f"{x:.3f}")
    )

    exibicao["maior_probabilidade"] = (
        exibicao["maior_probabilidade"]
        .map(lambda x: f"{x:.3f}")
    )

    print(
        exibicao.to_string(index=False)
    )

    # ------------------------------------------------------------------
    # 8. Destaques
    # ------------------------------------------------------------------

    uf_maior_percentual = analise.iloc[0]

    uf_maior_probabilidade = analise.loc[
        analise["probabilidade_media"].idxmax()
    ]

    print()
    print("=" * 70)
    print("DESTAQUES")
    print("=" * 70)

    print(
        f"\nUF com maior percentual de municípios em risco: "
        f"{uf_maior_percentual['sigla_uf']} "
        f"({uf_maior_percentual['percentual_em_risco']:.1f}%)"
    )

    print(
        f"UF com maior probabilidade média de risco: "
        f"{uf_maior_probabilidade['sigla_uf']} "
        f"({uf_maior_probabilidade['probabilidade_media']:.3f})"
    )

    print()
    print(
        f"[INFO] Resultado salvo em: {OUTPUT_PATH}"
    )

    print()
    print("=" * 70)
    print("ANÁLISE CONCLUÍDA")
    print("=" * 70)


if __name__ == "__main__":
    main()