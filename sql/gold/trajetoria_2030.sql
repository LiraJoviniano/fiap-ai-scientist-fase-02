-- Situacao dos municipios em relacao a meta de 2030.
--
-- O indice de trajetoria compara o ritmo observado entre 2023 e 2024 com
-- o necessario para alcancar 80% em 2030. Acima de 1, o municipio avanca
-- mais rapido do que precisa.
--
-- Duas ressalvas acompanham qualquer leitura destes numeros:
--
-- 1. Dois pontos no tempo dao variacao, nao tendencia. O ritmo necessario
--    pressupoe progresso linear, quando em educacao os ultimos pontos
--    percentuais sao os mais dificeis.
--
-- 2. O RS esta excluido. Ver anomalia_rs_*.sql: 89,6% dos municipios
--    gauchos cairam com mediana de -19,8, deslocamento uniforme que
--    indica alteracao na origem do dado. Incluido, ele infla artificial-
--    mente a classe de retrocesso em cerca de 400 municipios.

SELECT
    classificacao_trajetoria,
    COUNT(*)                                            AS municipios,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)  AS percentual,
    ROUND(AVG(taxa_2024), 1)                            AS taxa_media_2024,
    ROUND(AVG(variacao_anual), 1)                       AS variacao_media
FROM trajetoria_meta_2030
WHERE sigla_uf <> 'RS'
GROUP BY classificacao_trajetoria
ORDER BY municipios DESC
