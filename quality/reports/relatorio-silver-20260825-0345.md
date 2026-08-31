# Relatorio de qualidade — camada Silver

**Execucao:** 25/08/2026 03:45  
**Origem:** Spark SQL sobre o Glue Catalog (`alfabetizacao_silver`)  
**Situacao:** APROVADA  
**Regras:** 10 aprovadas · 0 reprovadas · 0 bloqueios

---

## Resultados

| Regra | Severidade | Situacao | Esperado | Obtido |
|---|---|---|---|---|
| **Q1** Integridade referencial de id_municipio | bloqueante | aprovado | 0 municipios orfaos | 0 orfaos |
| **Q2** Identificadores como texto de 7 digitos | bloqueante | aprovado | tipo string, todos com 7 digitos | tipo string, 0 fora do padrao |
| **Q3** Unicidade da chave natural | bloqueante | aprovado | nenhuma duplicata | 0 chaves duplicadas |
| **Q4** Vinculo territorial derivado do codigo IBGE | bloqueante | aprovado | todos com UF e regiao | 0 sem vinculo |
| **Q5** Coerencia com o ponto de corte 743 | bloqueante | aprovado | 0 divergencias | 0 em 3354661 registros |
| **Q6** Cobertura temporal e territorial | alerta | aprovado | 5550 municipios, 2 anos | 5550 municipios, 2 anos |
| **Q7** Nulos estruturais na distribuicao por nivel | alerta | aprovado | distribuicao apenas em 2024 | 0 linhas fora do padrao |
| **Q8** Conservacao de volume entre camadas | bloqueante | aprovado | bronze = silver | bronze 23995, silver 23995 |
| **Q9** Unicidade da chave em fato_escola | bloqueante | aprovado | nenhuma duplicata | 0 chaves duplicadas |
| **Q10** Coerencia territorial do Censo Escolar | alerta | aprovado | 0 divergencias de UF, 0 municipios fora da dimensao | 0 divergencias, 0 orfaos, 20 municipios so no Censo |

## Composicao da integracao

| Situacao | Linhas | Municipios |
|---|---:|---:|
| ano_base | 5448 | 5448 |
| comparavel | 5232 | 5232 |
| meta_nao_publicada | 120 | 120 |
| municipio_sem_meta | 96 | 96 |

## Resultado analitico

Entre os municipios com meta publicada para o ano, **2788 de 5232 (53.3%)** atingiram a meta na rede Municipal.

---

Gerado pelo Glue Job de qualidade. As consultas sao Spark SQL sobre o Catalog e podem ser reexecutadas por qualquer pessoa com acesso a ele — inclusive no Athena, que le o mesmo metastore.
