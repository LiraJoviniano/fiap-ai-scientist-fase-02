-- Evolucao da taxa media por UF, ano a ano.
--
-- Le a tabela em formato longo, que e o que um grafico de linha consome.
--
-- O RS aparece com queda de cerca de 20 pontos entre 2023 e 2024. A
-- investigacao em anomalia_rs_*.sql descartou participacao e composicao
-- da base como causas: a queda atinge 89,6% dos municipios do estado, de
-- forma uniforme. Trate como anomalia de origem, nao como resultado.

SELECT
    sigla_uf,
    ano,
    COUNT(*)                          AS municipios,
    ROUND(AVG(taxa_alfabetizacao), 1) AS taxa_media,
    ROUND(AVG(variacao_absoluta), 1)  AS variacao_media
FROM evolucao_temporal
WHERE sigla_uf IS NOT NULL
GROUP BY sigla_uf, ano
ORDER BY sigla_uf, ano
