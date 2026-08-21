-- Anomalia do RS: a queda vem de mudanca na composicao da base?
--
-- Segunda hipotese metodologica: se os municipios que sairam da base
-- entre 2023 e 2024 fossem justamente os de melhor desempenho, a media
-- cairia sem que nenhum municipio tivesse piorado.
--
-- Roda no database da Gold.
--
-- RESULTADO (execucao de 21/08/2026): hipotese INSUFICIENTE.
--   468 municipios com taxa em 2023
--   25 sairam, com media de 78,5% contra 72,4% dos que ficaram
-- Os que sairam eram 6 pontos melhores, mas sobre 25 de 468 o efeito na
-- media geral e inferior a meio ponto. Nao explica os 20 pontos.

SELECT
    COUNT(*)                                                   AS municipios_com_2023,
    ROUND(AVG(CASE WHEN taxa_2024 IS NOT NULL
                   THEN taxa_2023 END), 1)                     AS taxa2023_dos_que_ficaram,
    ROUND(AVG(CASE WHEN taxa_2024 IS NULL
                   THEN taxa_2023 END), 1)                     AS taxa2023_dos_que_sairam,
    SUM(CASE WHEN taxa_2024 IS NULL THEN 1 ELSE 0 END)         AS sairam
FROM trajetoria_meta_2030
WHERE sigla_uf = 'RS'
  AND taxa_2023 IS NOT NULL
