-- Anomalia do RS: a queda esta concentrada ou distribuida?
--
-- Terceira verificacao, depois de descartadas participacao e composicao.
-- Se poucos municipios despencaram, pode ser problema pontual de coleta;
-- se a queda e generalizada, algo mudou na aplicacao ou no desempenho do
-- estado inteiro.
--
-- Compara a distribuicao da variacao no RS com a do resto do pais.

SELECT
    CASE WHEN sigla_uf = 'RS' THEN 'RS' ELSE 'demais UFs' END  AS recorte,
    COUNT(*)                                                   AS municipios,
    ROUND(AVG(variacao_anual), 1)                              AS variacao_media,
    ROUND(APPROX_PERCENTILE(variacao_anual, 0.25), 1)          AS p25,
    ROUND(APPROX_PERCENTILE(variacao_anual, 0.50), 1)          AS mediana,
    ROUND(APPROX_PERCENTILE(variacao_anual, 0.75), 1)          AS p75,
    SUM(CASE WHEN variacao_anual < 0 THEN 1 ELSE 0 END)        AS em_queda,
    ROUND(100.0 * SUM(CASE WHEN variacao_anual < 0 THEN 1 ELSE 0 END)
          / COUNT(*), 1)                                       AS pct_em_queda
FROM trajetoria_meta_2030
WHERE variacao_anual IS NOT NULL
GROUP BY CASE WHEN sigla_uf = 'RS' THEN 'RS' ELSE 'demais UFs' END
ORDER BY recorte
