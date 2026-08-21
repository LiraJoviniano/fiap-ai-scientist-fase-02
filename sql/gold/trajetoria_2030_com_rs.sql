-- Situacao dos municipios em relacao a meta de 2030, incluindo o RS.
--
-- Existe como contraponto a trajetoria_2030.sql, que exclui o estado.
-- Comparar as duas torna visivel o quanto a anomalia gaucha desloca o
-- panorama nacional — e deixa a decisao de exclusao auditavel em vez de
-- silenciosa.

SELECT
    classificacao_trajetoria,
    COUNT(*)                                            AS municipios,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)  AS percentual,
    ROUND(AVG(taxa_2024), 1)                            AS taxa_media_2024,
    ROUND(AVG(variacao_anual), 1)                       AS variacao_media
FROM trajetoria_meta_2030
GROUP BY classificacao_trajetoria
ORDER BY municipios DESC
