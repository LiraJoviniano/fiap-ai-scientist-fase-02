-- Relacao entre infraestrutura escolar e taxa de alfabetizacao.
--
-- Agrupa os municipios por quartil do indice de infraestrutura e compara
-- a taxa media. Nao estabelece causalidade — e uma leitura descritiva que
-- orienta quais variaveis merecem entrar na modelagem.

WITH quartis AS (
    SELECT
        alvo_taxa_2024,
        indice_infraestrutura,
        alunos_por_docente,
        NTILE(4) OVER (ORDER BY indice_infraestrutura) AS quartil
    FROM features_municipio
    WHERE indice_infraestrutura IS NOT NULL
      AND alvo_taxa_2024 IS NOT NULL
)
SELECT
    quartil,
    COUNT(*)                              AS municipios,
    ROUND(AVG(indice_infraestrutura), 1)  AS indice_medio,
    ROUND(AVG(alvo_taxa_2024), 1)         AS taxa_media,
    ROUND(AVG(alunos_por_docente), 1)     AS alunos_por_docente
FROM quartis
GROUP BY quartil
ORDER BY quartil
