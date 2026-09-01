# Dicionário de Dados

**Projeto:** Pipeline de Dados para Análise da Alfabetização no Brasil (Tech Challenge — Fase 2)
**Escopo:** as três camadas do data lake — **Bronze**, **Silver** e **Gold**.

> **Fonte oficial:** [Indicador Criança Alfabetizada — Base dos Dados](https://basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72)
> A tabela `dicionario` (27 registros, extraída junto às demais) é a fonte primária dos
> significados de código. Este documento organiza e complementa o que ela traz; em caso
> de divergência, **a tabela `dicionario` prevalece**.

**Bucket do data lake:** `s3://fiap-ai-scientist-fase-02/`

## Legenda de classificação de dado (LGPD)

| Classificação | Significado |
|---|---|
| 🟢 Público | Dado agregado/estatístico, sem risco de identificação individual |
| 🟡 Pseudonimizado | Identificador que não é PII direto, mas não deve ser exposto sem necessidade |
| 🔴 Sensível | Dado que, combinado a outros, poderia identificar uma criança |

## Códigos confirmados

Valores lidos da tabela `dicionario` e verificados contra os dados.

**`rede`** — os indicadores usam **código**; as tabelas de meta usam **texto**. É a
tradução que torna o join possível.

| Código | Significado | Aparece em |
|---|---|---|
| `0` | Total (Federal, Estadual, Municipal e Privada) | `municipio`, `uf` |
| `1` | Federal | — |
| `2` | Estadual | `municipio`, `uf`, `alunos` |
| `3` | Municipal | `municipio`, `uf`, `alunos`, `meta_alfabetizacao_municipio` (como texto `Municipal`) |
| `4` | Privada | `alunos` |
| `5` | Pública (Estadual e Municipal) | `municipio`, `uf`, `meta_alfabetizacao_uf` e `_brasil` (como texto `Pública`) |
| `6` | Pública (Federal, Estadual e Municipal) | — |

**`serie`** — valor único `2`: 2º ano do Ensino Fundamental.

**`alfabetizado`** — `0` = Não, `1` = Sim. Equivale exatamente a `proficiencia >= 743`,
verificado em 3.354.661 registros com zero divergências.

**`presenca`** — `0` = Ausente, `1` = Presente.
**`preenchimento_caderno`** — `0` = Prova não preenchida, `1` = Prova preenchida.

---

# Camada Bronze

Dados extraídos sem transformação. Sete tabelas do Indicador Criança Alfabetizada mais
uma da fonte externa.

## `municipio`

**Origem:** `basedosdados.br_inep_avaliacao_alfabetizacao.municipio`
**Bronze:** `bronze/municipios/` · **Volume:** 23.995 linhas
**Granularidade:** município × ano × série × rede

Indicador de alfabetização agregado por município. **Não é dimensão territorial** — traz
apenas o código IBGE, sem nome de município nem sigla de UF.

| Coluna | Tipo | Descrição | Classificação |
|---|---|---|---|
| `ano` | INT | Ano de referência: 2023 ou 2024 | 🟢 |
| `id_municipio` | STRING (7 dígitos) | Código IBGE | 🟢 |
| `serie` | STRING | Sempre `2` | 🟢 |
| `rede` | STRING | Código da rede: `0`, `2`, `3` ou `5` | 🟢 |
| `taxa_alfabetizacao` | FLOAT (0–100) | Percentual que atingiu 743 pontos na escala Saeb | 🟢 |
| `media_portugues` | FLOAT | Proficiência média em Língua Portuguesa | 🟢 |
| `proporcao_aluno_nivel_0` … `_8` | FLOAT (0–100) | Distribuição pelos 9 níveis da escala Saeb | 🟢 |

> ⚠️ As colunas de nível **só têm valor em 2024**. Os 48,1% de nulos correspondem
> integralmente a 2023 — ausência estrutural de publicação, não falha de dado.

## `uf`

**Origem:** `…avaliacao_alfabetizacao.uf` · **Bronze:** `bronze/alfabetizacao/`
**Volume:** 145 linhas · **Granularidade:** UF × ano × série × rede

Mesmas colunas de `municipio`, com `sigla_uf` no lugar de `id_municipio`.

> ⚠️ Cobre **25 das 27 UFs**. DF e RR não aparecem. A ausência do DF é explicável — não
> possui rede municipal; a de RR fica registrada como lacuna da fonte.

## `alunos`

**Origem:** `…avaliacao_alfabetizacao.alunos` · **Bronze:** `bronze/alunos/`
**Volume:** 3.867.999 linhas · **Granularidade:** aluno × ano

Microdados individuais da avaliação — a tabela de maior sensibilidade do projeto.

| Coluna | Tipo | Descrição | Classificação |
|---|---|---|---|
| `ano` | INT | Ano da avaliação | 🟢 |
| `id_municipio` | STRING (7) | Código IBGE do município | 🟢 |
| `id_escola` | STRING | Código INEP da escola | 🟡 |
| `id_aluno` | STRING | Identificador sintético; **não é único isoladamente** — o mesmo aluno aparece em 2023 e 2024. A chave é `ano` + `id_aluno` | 🟡 |
| `caderno` | STRING | Caderno de prova aplicado (rotação de itens) | 🟢 |
| `serie` | STRING | Sempre `2` | 🟢 |
| `rede` | STRING | Código da rede: `2`, `3` ou `4` | 🟢 |
| `presenca` | STRING | `0` Ausente, `1` Presente | 🔴 |
| `preenchimento_caderno` | STRING | `0` não preenchida, `1` preenchida | 🔴 |
| `alfabetizado` | STRING | `1` se `proficiencia >= 743` | 🔴 |
| `proficiencia` | FLOAT | Pontuação na escala Saeb | 🔴 |
| `peso_aluno` | FLOAT | Peso amostral para estimativas agregadas | 🟢 |

> ⚠️ **Armadilha de agregação.** `alfabetizado` não tem nulos: os 512.153 alunos ausentes
> constam como `0`. Agregar sem filtrar trata ausência como reprovação e produz número
> divergente do oficial. É obrigatório filtrar `presenca = 1` e
> `preenchimento_caderno = 1`, e ponderar por `peso_aluno`.
>
> `proficiencia` é nula exatamente para quem não fez a prova: 512.153 ausentes mais 1.185
> presentes com caderno em branco explicam os 513.338 nulos, sem sobra.

## `meta_alfabetizacao_municipio`

**Origem:** `…avaliacao_alfabetizacao.meta_alfabetizacao_municipio`
**Bronze:** `bronze/metas_municipios/` · **Volume:** 10.704 linhas
**Granularidade:** município × safra de publicação

| Coluna | Tipo | Descrição | Classificação |
|---|---|---|---|
| `ano` | INT | **Safra de publicação**, não o ano da meta | 🟢 |
| `id_municipio` | STRING (7) | Código IBGE | 🟢 |
| `rede` | STRING | Texto `Municipal` — corresponde ao código `3` | 🟢 |
| `taxa_alfabetizacao` | FLOAT | Taxa observada na safra | 🟢 |
| `meta_alfabetizacao_2024` … `_2030` | FLOAT (0–100) | Meta pactuada para cada ano-alvo | 🟢 |
| `nivel_alfabetizacao` | INT (0–5) | Classificação do município na safra | 🟢 |
| `percentual_participacao` | FLOAT | Participação na avaliação | 🟢 |

> ⚠️ **A meta final de 2030 é 80,0%**, igual para todos os entes — valor único na coluna.
>
> ⚠️ **A série de metas começa em 2024.** Não existe `meta_alfabetizacao_2023`: 2023 é o
> ano-base do Compromisso.
>
> ⚠️ **As safras revisam as metas.** A safra de 2025 arredondou os alvos nacionais — a
> meta de 2024 passou de 59,9 para 60,0. A Silver adota a safra mais recente.

## Demais tabelas da Bronze

| Tabela | Caminho | Linhas | Descrição |
|---|---|---:|---|
| `meta_alfabetizacao_brasil` | `bronze/metas_brasil/` | 3 | Meta nacional, uma linha por safra |
| `meta_alfabetizacao_uf` | `bronze/metas_uf/` | 81 | Meta por UF — rede `Pública` (código `5`) |
| `dicionario` | `bronze/dicionario/` | 27 | Metadados oficiais da fonte |
| `censo_escolar` | `bronze/censo_escolar/` | 433.170 | Fonte externa — ver abaixo |

## `censo_escolar` — fonte externa

**Origem:** `basedosdados.br_inep_censo_escolar.escola`
**Bronze:** `bronze/censo_escolar/` · **Granularidade:** escola × ano

Recorte deliberado de **24 colunas das 455** originais e dois anos, o que reduz a
varredura de 6,17 GB para 56 MB. A seleção de colunas responde por quase toda a economia:
o BigQuery cobra por coluna varrida.

| Grupo | Colunas |
|---|---|
| Chaves | `ano`, `id_escola`, `id_municipio`, `sigla_uf`, `rede`, `tipo_localizacao`, `tipo_situacao_funcionamento` |
| Universo | `etapa_ensino_fundamental_anos_iniciais` |
| Contagens | `quantidade_matricula_fundamental_anos_iniciais` e `_integral`, `quantidade_docente_fundamental_anos_iniciais`, `quantidade_turma_fundamental_anos_iniciais`, `quantidade_matricula_zona_residencia_rural`, `quantidade_matricula_utiliza_transporte_publico` |
| Infraestrutura | `biblioteca`, `biblioteca_sala_leitura`, `laboratorio_informatica`, `banda_larga`, `agua_rede_publica`, `agua_potavel`, `energia_rede_publica`, `esgoto_rede_publica`, `esgoto_fossa_septica`, `alimentacao` |

**`tipo_situacao_funcionamento`** — `1` Em atividade (361.295), `2` Paralisada (63.404),
`3` Extinta (8.471). **17% das escolas não estão em atividade**; agregar sem filtrar
inflaria o denominador de toda proporção municipal.

---

# Camada Silver

Dados limpos, padronizados e integrados. Nove tabelas, com schema declarado
explicitamente no Glue Catalog (`alfabetizacao_silver`).

**Princípio que atravessa a camada: a Silver marca, não filtra.** Colunas como
`aluno_valido`, `em_atividade` e `oferta_anos_iniciais` sinalizam o recorte pertinente,
mas nenhuma linha é descartada — a decisão é de quem agrega, e a coluna torna o filtro
impossível de esquecer.

## `dim_territorio`

**Granularidade:** município · **Volume:** 5.570 linhas

Dimensão construída pela **união** do indicador e do Censo. Dimensão cobre o universo;
incompletude pertence aos fatos.

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_municipio` | STRING | Código IBGE. Chave da dimensão |
| `codigo_uf` | STRING | Dois primeiros dígitos do código IBGE |
| `sigla_uf` | STRING | Derivada de `codigo_uf` |
| `regiao` | STRING | Norte, Nordeste, Sudeste, Sul ou Centro-Oeste |
| `nome_municipio` | STRING | **Sempre nulo** — não existe em nenhuma das fontes |
| `tem_indicador` | BOOLEAN | Município presente na avaliação de alfabetização |
| `tem_censo` | BOOLEAN | Município presente no Censo Escolar |

> **20 municípios têm `tem_censo = true` e `tem_indicador = false`** — existem no Censo e
> não têm resultado de alfabetização. A coluna torna essa diferença mensurável.
>
> `nome_municipio` é declarada vazia em vez de omitida, para que a lacuna seja explícita.
> Preenchê-la exige extrair `br_bd_diretorios_brasil.municipio`.

## `dim_rede`

**Granularidade:** código de rede · **Volume:** 7 linhas

| Coluna | Tipo | Descrição |
|---|---|---|
| `rede_codigo` | STRING | `0` a `6` |
| `rede_nome` | STRING | Nome curto, para eixos e legendas |
| `rede_descricao` | STRING | Descrição completa da fonte |

## `fato_indicador_municipio`

**Chave natural:** `ano` + `id_municipio` + `rede_codigo` · **Volume:** 23.995 linhas

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | INT | 2023 ou 2024 |
| `id_municipio` | STRING | Código IBGE |
| `codigo_uf`, `sigla_uf`, `regiao` | STRING | Derivados do código IBGE |
| `serie` | STRING | Sempre `2` |
| `rede_codigo`, `rede_nome` | STRING | Código e nome traduzido |
| `taxa_alfabetizacao` | DOUBLE | Percentual alfabetizado |
| `media_portugues` | DOUBLE | Proficiência média |
| `tem_distribuicao_nivel` | BOOLEAN | Se a linha tem distribuição por nível |
| `proporcao_aluno_nivel_0` … `_8` | DOUBLE | Distribuição por nível |

> Use `tem_distribuicao_nivel` para filtrar, em vez de testar nulo coluna a coluna.

## `fato_indicador_uf`

**Chave natural:** `ano` + `sigla_uf` + `rede_codigo` · **Volume:** 145 linhas

Mesma estrutura, sem `id_municipio` e `codigo_uf` — a derivação territorial depende do
código de município, ausente aqui.

## `fato_aluno`

**Chave natural:** `ano` + `id_aluno` · **Volume:** 3.867.999 linhas

| Coluna | Tipo | Descrição | Classificação |
|---|---|---|---|
| `ano` | INT | Ano da avaliação | 🟢 |
| `id_aluno` | STRING | Identificador sintético | 🟡 |
| `id_municipio`, `id_escola` | STRING | Território e escola | 🟡 |
| `codigo_uf`, `sigla_uf`, `regiao` | STRING | Derivados do código IBGE | 🟢 |
| `serie`, `caderno` | STRING | Série e caderno de prova | 🟢 |
| `rede_codigo`, `rede_nome` | STRING | Rede de ensino | 🟢 |
| `presente` | BOOLEAN | Esteve presente na prova | 🔴 |
| `prova_preenchida` | BOOLEAN | Caderno preenchido | 🔴 |
| `aluno_valido` | BOOLEAN | `presente` **e** `prova_preenchida` | 🔴 |
| `alfabetizado` | BOOLEAN | Atingiu 743 pontos | 🔴 |
| `proficiencia` | DOUBLE | Pontuação na escala Saeb | 🔴 |
| `distancia_corte` | DOUBLE | `proficiencia − 743` | 🔴 |
| `faixa_proximidade` | STRING | Ver abaixo | 🔴 |
| `peso_aluno` | DOUBLE | Peso amostral | 🟢 |

**`faixa_proximidade`** classifica a distância até o corte, em faixas de 50 pontos:
`muito_abaixo`, `proximo_abaixo`, `proximo_acima`, `muito_acima` e `nao_avaliado`. A faixa
`proximo_abaixo` reúne os alunos a menos de 50 pontos do corte — o grupo com maior retorno
marginal de intervenção pedagógica, e o que a média esconde.

> ⚠️ **Filtro obrigatório antes de qualquer agregação:** `aluno_valido = true`, ponderando
> por `peso_aluno`.

## `fato_escola`

**Chave natural:** `ano` + `id_escola` · **Volume:** 433.170 linhas

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | INT | 2023 ou 2024 |
| `id_escola` | STRING | Código INEP |
| `id_municipio`, `codigo_uf`, `regiao` | STRING | Território |
| `sigla_uf` | STRING | **Da origem**, declarada pelo Censo |
| `sigla_uf_derivada` | STRING | **Derivada** do código IBGE, para conferência cruzada |
| `rede_codigo`, `rede_nome` | STRING | Rede de ensino |
| `tipo_localizacao` | STRING | Urbana ou rural |
| `situacao_funcionamento` | STRING | Código da origem: `1`, `2` ou `3` |
| `em_atividade` | BOOLEAN | `situacao_funcionamento = '1'` |
| `oferta_anos_iniciais` | BOOLEAN | Escola oferta anos iniciais do fundamental |
| `matriculas_anos_iniciais` | INT | Base da ponderação nas agregações |
| `matriculas_integral` | INT | Matrículas em tempo integral |
| `docentes_anos_iniciais` | INT | Docentes nos anos iniciais |
| `turmas_anos_iniciais` | INT | Turmas nos anos iniciais |
| `matriculas_zona_rural` | INT | Alunos residentes em zona rural |
| `matriculas_transporte` | INT | Alunos que usam transporte público |
| `tem_biblioteca` … `tem_alimentacao` | BOOLEAN | Sete flags de infraestrutura |

> ⚠️ **Filtros obrigatórios antes de agregar:** `em_atividade` e `oferta_anos_iniciais`.
> Ponderar por `matriculas_anos_iniciais` — uma escola de 800 alunos não equivale a uma
> de 20.
>
> `sigla_uf` e `sigla_uf_derivada` são duas fontes independentes para o mesmo fato
> territorial. A regra Q10 compara as duas: zero divergências em 433.170 escolas.

## `fato_meta`

**Volume:** 37.660 linhas (de 75.516 após unpivot, resolvidas as safras)

| Coluna | Tipo | Descrição |
|---|---|---|
| `nivel_territorial` | STRING | `municipio`, `uf` ou `brasil` |
| `safra` | INT | Ano de **publicação** da meta |
| `ano_meta` | INT | Ano-**alvo**: 2024 a 2030 |
| `id_municipio` | STRING | Preenchido só no nível municipal |
| `sigla_uf` | STRING | Preenchido só no nível de UF |
| `rede_codigo`, `rede_nome` | STRING | Traduzidos do texto da origem |
| `meta_alfabetizacao` | DOUBLE | Meta para `ano_meta` |
| `taxa_alfabetizacao` | DOUBLE | Taxa realizada na safra |
| `percentual_participacao` | DOUBLE | Participação na avaliação |
| `nivel_alfabetizacao` | INT | Classificação do ente (0–5) |

> ⚠️ **Distinguir `safra` de `ano_meta` é essencial.** Confundi-los produz comparações sem
> sentido. Para cada ente e ano-alvo, prevalece a safra mais recente.

## `meta_vs_resultado`

**Granularidade:** município × ano, rede Municipal · **Volume:** 10.896 linhas

Colunas de `fato_indicador_municipio` filtrado para `rede_codigo = '3'`, mais:

| Coluna | Tipo | Descrição |
|---|---|---|
| `meta_alfabetizacao` | DOUBLE | Meta do ano; nula quando não há |
| `nivel_alfabetizacao` | INT | Classificação na tabela de metas |
| `distancia_meta` | DOUBLE | `taxa_alfabetizacao − meta_alfabetizacao` |
| `atingiu_meta` | BOOLEAN **nullable** | `True`, `False` ou `<NA>` |
| `situacao_meta` | STRING | Ver abaixo |

**Valores de `situacao_meta`:**

| Valor | Linhas | Significado | Quarentena |
|---|---:|---|---|
| `ano_base` | 5.448 | 2023 não tem meta por definição | Não |
| `comparavel` | 5.232 | Resultado e meta disponíveis | Não |
| `meta_nao_publicada` | 120 | Município consta na tabela, meta do ano é nula | Sim |
| `municipio_sem_meta` | 96 | Município ausente da tabela de metas | Sim |

> ⚠️ **`atingiu_meta` é boolean nullable, não bool.** Onde não há meta o valor é `<NA>`, e
> não `False`. Agregar sem tratar o nulo classificaria 216 municípios como "não atingiram
> a meta" — afirmação falsa sobre 216 entes.
>
> **Resultado de referência:** entre as 5.232 linhas `comparavel`, 2.788 atingiram a meta
> (53,3%).

## `quarentena`

**Volume:** 216 linhas

Mesma estrutura de `meta_vs_resultado`, mais `motivo_quarentena` e `origem`.

> Quarentena não é descarte: são linhas contabilizadas, com o motivo registrado. A regra
> Q8 verifica que entrada é igual a aprovados mais quarentena. Um município aqui é um
> município que precisa de meta publicada, não um município a ignorar.

---

# Camada Gold

Datasets agregados para consumo, no database `alfabetizacao_gold`. A Gold agrega e não
limpa — a padronização aconteceu na Silver.

## `indicador_municipio`

**Granularidade:** município × ano × rede

Série completa, todas as redes. Colunas de `fato_indicador_municipio` mais
`meta_alfabetizacao`, `distancia_meta`, `atingiu_meta` e `situacao_meta`.

> Só a rede Municipal tem meta no nível municipal; nas demais o campo fica nulo, o que é
> mais honesto que omitir a linha.

## `evolucao_temporal`

**Granularidade:** município × ano · **Formato longo**

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_municipio`, `sigla_uf`, `regiao` | STRING | Território |
| `ano` | INT | Ano de referência |
| `taxa_alfabetizacao` | DOUBLE | Taxa do ano |
| `taxa_ano_anterior` | DOUBLE | Taxa do ano anterior; nula no primeiro |
| `variacao_absoluta` | DOUBLE | Diferença em pontos percentuais |
| `variacao_percentual` | DOUBLE | Variação relativa; nula se a taxa anterior é zero |
| `taxa_ano_base` | DOUBLE | Taxa do primeiro ano da série |
| `variacao_acumulada` | DOUBLE | Diferença em relação ao ano-base |
| `meta_alfabetizacao`, `distancia_meta`, `situacao_meta` | — | Contexto da meta |

> Com dois anos de cobertura, `variacao_acumulada` coincide com `variacao_absoluta` em
> 2024. As duas colunas existem porque a estrutura continua correta quando a fonte
> publicar a edição seguinte.

## `trajetoria_meta_2030`

**Granularidade:** município

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_municipio`, `sigla_uf`, `regiao` | STRING | Território |
| `taxa_2023`, `taxa_2024` | DOUBLE | Taxas dos dois anos |
| `variacao_anual` | DOUBLE | `taxa_2024 − taxa_2023` |
| `meta_2024` | DOUBLE | Meta do ano de referência |
| `atingiu_meta_2024` | BOOLEAN | Atingimento em 2024 |
| `distancia_meta_2030` | DOUBLE | `80,0 − taxa_2024` |
| `ritmo_necessario` | DOUBLE | `distancia_meta_2030 ÷ 6` |
| `indice_trajetoria` | DOUBLE | `variacao_anual ÷ ritmo_necessario` |
| `classificacao_trajetoria` | STRING | Ver abaixo |
| `elegivel_meta` | BOOLEAN | `situacao_meta = 'comparavel'` |

**Valores de `classificacao_trajetoria`:**

| Valor | Municípios | Significado |
|---|---:|---|
| `retrocesso` | 1.917 | Taxa caiu entre 2023 e 2024 |
| `em_ritmo` | 1.704 | Avança no ritmo necessário ou acima |
| `meta_atingida` | 1.147 | Já alcançou os 80% |
| `ritmo_insuficiente` | 536 | Avança, mas abaixo do necessário |
| `sem_meta` | 196 | Trajetória não avaliável |

> **`indice_trajetoria` é nulo** quando o município já alcançou os 80%: o ritmo necessário
> seria zero ou negativo, e o índice ficaria indefinido.
>
> ⚠️ **Duas ressalvas de leitura.** Dois pontos no tempo dão variação, não tendência. E o
> ritmo necessário pressupõe progresso linear, quando em educação os últimos pontos
> percentuais são os mais difíceis.
>
> ⚠️ **O Rio Grande do Sul é anomalia conhecida.** 89,6% dos municípios gaúchos caíram,
> com mediana de −19,8 e p75 de −10,5 — a distribuição inteira se deslocou. As hipóteses
> de participação e de composição da base foram testadas e descartadas. Uniformidade
> dessa ordem indica alteração na origem do dado. As consultas de trajetória separam o
> estado; ver `sql/gold/anomalia_rs_*.sql`.

## `features_municipio`

**Granularidade:** município · Insumo de modelagem

| Grupo | Colunas |
|---|---|
| Chaves | `id_municipio`, `sigla_uf`, `regiao` |
| Alvos | `alvo_taxa_2024` (contínuo), `alvo_atingiu_meta` (binário) |
| Elegibilidade | `elegivel_meta` |
| Histórico | `taxa_2023`, `variacao_anual` |
| Escala | `total_escolas`, `total_matriculas` |
| Pedagógicas | `alunos_por_docente`, `alunos_por_turma`, `pct_matricula_integral` |
| Infraestrutura | `pct_matricula_biblioteca`, `_lab_informatica`, `_banda_larga`, `_agua_adequada`, `_energia_publica`, `_esgoto_adequado`, `_alimentacao`, `indice_infraestrutura` |
| Contexto | `pct_matricula_rural`, `pct_matricula_transporte`, `pct_escolas_urbanas` |

**Dois alvos, para que a escolha entre regressão e classificação seja de quem modela.**
`elegivel_meta` distingue quem pode entrar num modelo de risco: sem meta publicada não há
classe a prever.

**As variáveis de infraestrutura são ponderadas por matrícula**, não por contagem de
escolas. O universo é restrito a escolas em atividade que ofertam anos iniciais na rede
Municipal.

**`indice_infraestrutura`** é a média simples de seis dimensões — alimentação fica de
fora por ser quase universal e não discriminar entre municípios.

> ⚠️ **`variacao_anual` não deve ser usada como preditor** de `alvo_atingiu_meta` nem da
> classificação de trajetória. Ela é o próprio insumo do cálculo do alvo: usá-la faz o
> modelo aprender a regra de construção, não o padrão real. Ver ADR-006 no README.

---

## Governança do dado individual

A tabela `alunos` é a de maior sensibilidade do projeto. Apesar de não conter nome, CPF
ou qualquer identificador direto, a combinação `id_escola` + `ano` + `serie` permitiria,
em municípios pequenos com poucas turmas, estreitar o universo até um indivíduo.

**Decisão adotada: o grão individual é preservado na Silver, com contenção documentada.**

`silver.fato_aluno` mantém 3.867.999 linhas no grão do estudante porque a análise de
distribuição por faixa de proficiência — identificar a massa de alunos imediatamente
abaixo do corte de 743 — não é possível a partir de agregados municipais. É justamente o
grupo com maior retorno marginal de intervenção pedagógica, e a média o esconde.

As contenções que sustentam a decisão:

| Contenção | Situação |
|---|---|
| Ausência de identificador direto | `id_aluno` é sintético, atribuído pela avaliação |
| Limite de propagação | **A Gold não lê `fato_aluno`.** O Job da camada consome apenas `fato_indicador_municipio`, `fato_escola`, `meta_vs_resultado` e `dim_territorio` |
| Exposição analítica | Todos os datasets da Gold são agregados por município — nenhum expõe registro individual |
| Acesso | Restrito à conta AWS do projeto, com bloqueio de acesso público no bucket |
| Não versionamento | Nenhum Parquet vai para o repositório; o `.gitignore` bloqueia `data/**/*.parquet` |

**O grão individual começa e termina na Silver.** Essa é a fronteira que torna a decisão
defensável: o dado sensível existe onde é necessário para a análise e não se propaga para
a camada de consumo.

Ponto de revisão: se em algum momento um consumidor da Gold precisar de informação no
grão do aluno, esta decisão deve ser reavaliada antes de atendê-lo — provavelmente pela
remoção de `id_escola`, que é o campo que viabiliza o estreitamento.

---

## Como conferir os códigos na fonte

```bash
python -c "import pandas as pd; df = pd.read_parquet('data/bronze/dicionario/dicionario.parquet'); print(df.to_string())"
```