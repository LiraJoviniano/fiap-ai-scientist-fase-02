# Governança e FinOps

## Escopo entregue

Esta parte complementa o dicionário de dados, a linhagem e as regras de qualidade Q1–Q10 que já existem no projeto.

| Pilar | Implementação | Evidência |
|---|---|---|
| Data Quality | Great Expectations na Bronze e regras Q1–Q10 na Silver | `quality/` e `quality/reports/` |
| Observabilidade | Logs estruturados em JSON e manifesto por execução | `reports/governance/execucao-*.json` |
| Segurança | Auditoria somente leitura dos controles do S3 | `reports/governance/auditoria-s3.json` |
| Rastreabilidade | ID, horários, duração, volume, status e erro | Manifesto JSON da execução |
| FinOps | Inventário real de objetos, classes e volume armazenado | `reports/finops/relatorio-finops.json` |
| Otimização | Parquet/Snappy e particionamento existentes na pipeline, complementados pelo lifecycle do S3 | Código da pipeline e `src/finops/analise_s3.py` |

## Execução no PowerShell

Com a `.venv` ativa e as credenciais temporárias do AWS Academy válidas, execute:

```powershell
python -m src.governance.auditoria_s3
python -m quality.run_quality_checks
python -m src.finops.analise_s3 relatorio
python -m src.finops.analise_s3 verificar
```

Esses comandos não alteram os objetos armazenados no bucket. Eles podem criar relatórios locais em `quality/reports/`, `reports/governance/` e `reports/finops/`.

## Aplicação do lifecycle

O lifecycle altera a configuração do bucket e exige confirmação explícita:

```powershell
python -m src.finops.analise_s3 aplicar-lifecycle --aplicar
```

Antes de aplicar, é possível consultar as regras existentes:

```powershell
python -m src.finops.analise_s3 verificar
```

O código preserva as regras existentes e substitui somente as duas regras FinOps deste projeto.

## Configuração do ambiente

O bucket e a região são obtidos do arquivo `.env`. Nenhum nome de bucket fica fixado no código.

Exemplo para um ambiente de desenvolvimento:

```ini
AWS_REGION=us-east-1
AWS_BUCKET=nome-do-bucket-de-desenvolvimento

FINOPS_RELATORIO_PREFIX=
FINOPS_LIFECYCLE_PREFIX=bronze/
FINOPS_TRANSICAO_IA_DIAS=30
FINOPS_TRANSICAO_GLACIER_DIAS=90

FINOPS_GLUE_DPU_HORAS_MES=1.84
FINOPS_ATHENA_GB_MES=0.1
FINOPS_KINESIS_SHARD_HORAS_MES=1
FINOPS_KINESIS_PUT_MILHAO_MES=0.000003
FINOPS_LAMBDA_REQUISICOES_MILHAO_MES=0.000001
FINOPS_LAMBDA_GB_SEGUNDOS_MES=0.000128
FINOPS_BIGQUERY_TIB_MES=0.1
```

Para usar outro ambiente, altere `AWS_BUCKET` e confirme que a credencial possui acesso ao bucket.

As credenciais do AWS Academy não devem ser colocadas no `.env`. Elas devem permanecer no arquivo local de credenciais da AWS CLI.

## Estratégia de otimização

O lifecycle implementa as seguintes regras para a camada Bronze:

| Regra | Configuração |
|---|---|
| Transição para Standard-IA | Objetos maiores que 128 KiB após 30 dias |
| Transição para Glacier Instant Retrieval | Objetos maiores que 128 KiB após 90 dias |
| Upload multipart incompleto | Cancelamento após 7 dias |
| Regras preexistentes | Preservadas durante a atualização |
| Regras FinOps duplicadas | Substituídas pela versão atual |

Arquivos menores que 128 KiB permanecem na classe Standard porque a transição pode custar mais que a economia gerada.

## Evidências da execução

A implementação foi validada no bucket de desenvolvimento após a execução das pipelines batch e streaming:

```text
10 testes unitários aprovados
32 objetos inventariados
190,001 MiB armazenados
2 regras de lifecycle ativas
```

### Volume por camada

| Camada | Objetos | Volume |
|---|---:|---:|
| Bronze | 9 | 73,987 MiB |
| Silver | 11 | 114,324 MiB |
| Gold | 6 | 1,619 MiB |
| Quality | 1 | 0,002 MiB |
| Scripts | 5 | 0,069 MiB |
| **Total** | **32** | **190,001 MiB** |

> **Observação:** Silver e Gold agora contêm resultados reais das pipelines batch e streaming. O inventário também inclui dois pequenos objetos controlados mantidos em `silver/teste_finops/` e `gold/teste_finops/`, utilizados na validação inicial do inventário. O impacto deles na estimativa é residual.

### Estimativa mensal por serviço

| Serviço | Custo mensal estimado (USD) |
|---|---:|
| Amazon S3 | 0,004268 |
| AWS Glue | 0,809600 |
| Amazon Athena | 0,000000 |
| Amazon Kinesis | 0,015000 |
| AWS Lambda | 0,000002 |
| BigQuery | 0,000000 |
| **Total estimado** | **0,828870** |

> **Observação:** os sete objetos da Bronze foram produzidos pela ingestão. Os objetos em Silver e Gold são cópias controladas utilizadas somente para validar se o inventário FinOps reconhece e separa corretamente as camadas. Eles não representam a execução completa das transformações Silver e Gold.

### Estimativa mensal por serviço

| Serviço | Custo mensal estimado (USD) |
|---|---:|
| Amazon S3 | 0,001533 |
| AWS Glue | 0,809600 |
| Amazon Athena | 0,000488 |
| Amazon Kinesis | 0,015000 |
| AWS Lambda | 0,000000 |
| BigQuery | 0,000000 |

A auditoria do bucket apresentou:

```text
[CONFORME] criptografia_em_repouso
[CONFORME] bloqueio_acesso_publico
[INFORMATIVO] versionamento: Disabled
[CONFORME] lifecycle_finops: 2 regras encontradas
```

O versionamento desativado foi classificado como informativo, e não como falha, pois sua ativação aumenta o volume armazenado e deve ser uma decisão conjunta de Governança e FinOps.

## Limites e decisões

- A estimativa é comparativa e não representa uma fatura oficial.
- O custo real deve ser confirmado no AWS Cost Explorer ou AWS Pricing Calculator.
- A estimativa inclui armazenamento no S3, Glue, Athena, Kinesis, Lambda e BigQuery.
- Os valores de uso mensal são premissas configuradas no `.env`, não medições da fatura.
- A estimativa não inclui requisições S3, recuperação de arquivos, permanência mínima, transferência ou impostos.
- O Glacier pode gerar custo de recuperação e exigir permanência mínima.
- A política de lifecycle não é aplicada automaticamente.
- O relatório não armazena credenciais, tokens ou conteúdo dos registros.
- As credenciais do AWS Academy são temporárias e nunca devem ser publicadas no Git.
