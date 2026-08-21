-- Anomalia do RS: a queda vem de menor participacao na avaliacao?
--
-- Entre 2023 e 2024 a taxa media do Rio Grande do Sul caiu cerca de 20
-- pontos percentuais, muito alem da variacao de qualquer outra UF. A
-- primeira hipotese e metodologica: se menos alunos fizeram a prova, a
-- media pode ter mudado sem que o aprendizado mudasse.
--
-- Le a Silver de forma qualificada, para rodar junto das demais
-- consultas da Gold. Mede participacao real — quantos alunos
-- estavam presentes e preencheram o caderno.
--
-- RESULTADO (execucao de 21/08/2026): hipotese DESCARTADA.
--   2023: 121.148 alunos, 81,4% de participacao
--   2024: 108.262 alunos, 82,7% de participacao
-- A participacao subiu. Note, porem, a queda de 11% no total de alunos.

SELECT
    ano,
    COUNT(*)                                                    AS alunos,
    SUM(CASE WHEN aluno_valido THEN 1 ELSE 0 END)               AS validos,
    ROUND(100.0 * SUM(CASE WHEN aluno_valido THEN 1 ELSE 0 END)
          / COUNT(*), 1)                                        AS pct_participacao
FROM alfabetizacao_silver.fato_aluno
WHERE sigla_uf = 'RS'
GROUP BY ano
ORDER BY ano
