-- Onde a taxa de alfabetizacao caiu entre 2023 e 2024.
--
-- Retrocesso e o achado mais relevante da camada Gold: politica publica
-- para municipio que avanca devagar e diferente de politica para
-- municipio que esta piorando.
--
-- O RS aparece separado por decisao metodologica, nao por conveniencia.
-- A investigacao em anomalia_rs_*.sql mostrou que 89,6% dos municipios
-- gauchos cairam, com mediana de -19,8 e p75 de -10,5: a distribuicao
-- inteira se deslocou. Uniformidade dessa ordem e assinatura de alteracao
-- na origem do dado, nao de variacao de aprendizado. Mante-lo na conta do
-- Sul produziria a conclusao falsa de que a regiao esta retrocedendo.

SELECT
    CASE WHEN sigla_uf = 'RS' THEN 'RS (anomalia)' ELSE regiao END AS recorte,
    COUNT(*)                                                       AS municipios,
    SUM(CASE WHEN classificacao_trajetoria = 'retrocesso'
             THEN 1 ELSE 0 END)                                    AS em_retrocesso,
    ROUND(100.0 * SUM(CASE WHEN classificacao_trajetoria = 'retrocesso'
             THEN 1 ELSE 0 END) / COUNT(*), 1)                     AS pct_retrocesso,
    ROUND(AVG(variacao_anual), 1)                                  AS variacao_media
FROM trajetoria_meta_2030
WHERE regiao IS NOT NULL
GROUP BY CASE WHEN sigla_uf = 'RS' THEN 'RS (anomalia)' ELSE regiao END
ORDER BY pct_retrocesso DESC
