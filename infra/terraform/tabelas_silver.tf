// ---------------------------------------------------------------------------
// Tabelas da camada Silver — schema explícito
//
// Diferente da Bronze, a Silver não usa Crawler. O schema desta camada é
// decisão, não descoberta: `atingiu_meta` é boolean porque "sem meta" não
// é "não atingiu"; `id_municipio` é string porque código IBGE não é número.
// Deixar um Crawler inferir isso terceirizaria a decisão para um palpite
// sobre os dados de uma execução específica.
//
// Declarar em Terraform, e não por DDL no Athena, tem uma vantagem
// concreta: se o schema do Job mudar, o `terraform plan` acusa a
// divergência e corrige. Um `CREATE TABLE IF NOT EXISTS` veria que a
// tabela existe e não faria nada, deixando o Catalog descrevendo uma coisa
// enquanto o Parquet contém outra.
//
// As definições abaixo espelham ESQUEMA_SILVER em
// src/transformation/silver.py. Se uma mudar, a outra precisa mudar junto.
// ---------------------------------------------------------------------------

locals {
  // As nove proporções por nível de proficiência. Só têm valor em 2024 —
  // ausência estrutural, sinalizada por tem_distribuicao_nivel.
  niveis_proficiencia = [
    for i in range(9) : {
      name = "proporcao_aluno_nivel_${i}"
      type = "double"
    }
  ]

  colunas_indicador_comuns = [
    { name = "serie", type = "string" },
    { name = "rede_codigo", type = "string" },
    { name = "rede_nome", type = "string" },
    { name = "taxa_alfabetizacao", type = "double" },
    { name = "media_portugues", type = "double" },
    { name = "tem_distribuicao_nivel", type = "boolean" },
  ]

  colunas_meta_vs_resultado = [
    { name = "ano", type = "int" },
    { name = "id_municipio", type = "string" },
    { name = "codigo_uf", type = "string" },
    { name = "sigla_uf", type = "string" },
    { name = "regiao", type = "string" },
    { name = "rede_codigo", type = "string" },
    { name = "rede_nome", type = "string" },
    { name = "taxa_alfabetizacao", type = "double" },
    { name = "media_portugues", type = "double" },
    { name = "meta_alfabetizacao", type = "double" },
    { name = "nivel_alfabetizacao", type = "int" },
    { name = "distancia_meta", type = "double" },
    { name = "atingiu_meta", type = "boolean" },
    { name = "situacao_meta", type = "string" },
  ]

  tabelas_silver = {
    dim_territorio = {
      location = "dimensoes/dim_territorio"
      columns = [
        { name = "id_municipio", type = "string" },
        { name = "codigo_uf", type = "string" },
        { name = "sigla_uf", type = "string" },
        { name = "regiao", type = "string" },
        // O nome do município não existe em nenhuma das fontes. A coluna é
        // declarada em vez de omitida, para que a lacuna fique explícita.
        { name = "nome_municipio", type = "string" },
        // A dimensão cobre o universo das duas fontes: o Censo alcança
        // municípios ausentes da avaliação. As flags tornam a diferença
        // de cobertura mensurável em vez de silenciosa.
        { name = "tem_indicador", type = "boolean" },
        { name = "tem_censo", type = "boolean" },
      ]
    }

    dim_rede = {
      location = "dimensoes/dim_rede"
      columns = [
        { name = "rede_codigo", type = "string" },
        { name = "rede_nome", type = "string" },
        { name = "rede_descricao", type = "string" },
      ]
    }

    fato_indicador_municipio = {
      location = "fatos/fato_indicador_municipio"
      columns = concat(
        [
          { name = "ano", type = "int" },
          { name = "id_municipio", type = "string" },
          { name = "codigo_uf", type = "string" },
          { name = "sigla_uf", type = "string" },
          { name = "regiao", type = "string" },
        ],
        local.colunas_indicador_comuns,
        local.niveis_proficiencia,
      )
    }

    fato_indicador_uf = {
      location = "fatos/fato_indicador_uf"
      columns = concat(
        [
          { name = "ano", type = "int" },
          { name = "sigla_uf", type = "string" },
          { name = "regiao", type = "string" },
        ],
        local.colunas_indicador_comuns,
        local.niveis_proficiencia,
      )
    }

    // Grao do estudante: 3,9 milhoes de linhas. Nao filtra ausentes —
    // marca com aluno_valido, para que a taxa de participacao continue
    // mensuravel. Quem agrega decide o recorte.
    fato_aluno = {
      location = "fatos/fato_aluno"
      columns = [
        { name = "ano", type = "int" },
        { name = "id_aluno", type = "string" },
        { name = "id_municipio", type = "string" },
        { name = "id_escola", type = "string" },
        { name = "codigo_uf", type = "string" },
        { name = "sigla_uf", type = "string" },
        { name = "regiao", type = "string" },
        { name = "serie", type = "string" },
        { name = "caderno", type = "string" },
        { name = "rede_codigo", type = "string" },
        { name = "rede_nome", type = "string" },
        { name = "presente", type = "boolean" },
        { name = "prova_preenchida", type = "boolean" },
        // Filtro obrigatorio antes de qualquer agregacao: a coluna
        // alfabetizado vale false para quem nao fez a prova.
        { name = "aluno_valido", type = "boolean" },
        { name = "alfabetizado", type = "boolean" },
        { name = "proficiencia", type = "double" },
        { name = "distancia_corte", type = "double" },
        // Faixa em relacao ao corte de 743. proximo_abaixo e o grupo com
        // maior retorno marginal de intervencao pedagogica.
        { name = "faixa_proximidade", type = "string" },
        { name = "peso_aluno", type = "double" },
      ]
    }

    // Grao da escola, a partir do Censo Escolar. A Silver limpa e
    // padroniza; a agregacao para municipio, ponderada por matricula,
    // fica na Gold.
    fato_escola = {
      location = "fatos/fato_escola"
      columns = [
        { name = "ano", type = "int" },
        { name = "id_escola", type = "string" },
        { name = "id_municipio", type = "string" },
        { name = "codigo_uf", type = "string" },
        // sigla_uf vem da origem; a derivada permite conferir divergencia
        { name = "sigla_uf", type = "string" },
        { name = "sigla_uf_derivada", type = "string" },
        { name = "regiao", type = "string" },
        { name = "rede_codigo", type = "string" },
        { name = "rede_nome", type = "string" },
        { name = "tipo_localizacao", type = "string" },
        // O Censo mantem escolas paralisadas e extintas: a Gold deve
        // agregar apenas as em atividade.
        { name = "situacao_funcionamento", type = "string" },
        { name = "em_atividade", type = "boolean" },
        { name = "oferta_anos_iniciais", type = "boolean" },
        { name = "matriculas_anos_iniciais", type = "int" },
        { name = "matriculas_integral", type = "int" },
        { name = "docentes_anos_iniciais", type = "int" },
        { name = "turmas_anos_iniciais", type = "int" },
        { name = "matriculas_zona_rural", type = "int" },
        { name = "matriculas_transporte", type = "int" },
        { name = "tem_biblioteca", type = "boolean" },
        { name = "tem_laboratorio_informatica", type = "boolean" },
        { name = "tem_banda_larga", type = "boolean" },
        { name = "tem_agua_adequada", type = "boolean" },
        { name = "tem_energia_publica", type = "boolean" },
        { name = "tem_esgoto_adequado", type = "boolean" },
        { name = "tem_alimentacao", type = "boolean" },
      ]
    }

    fato_meta = {
      location = "fatos/fato_meta"
      columns = [
        { name = "nivel_territorial", type = "string" },
        // safra é o ano de publicação; ano_meta é o ano-alvo. Confundir
        // os dois produz comparações sem sentido.
        { name = "safra", type = "int" },
        { name = "ano_meta", type = "int" },
        { name = "id_municipio", type = "string" },
        { name = "sigla_uf", type = "string" },
        { name = "rede_codigo", type = "string" },
        { name = "rede_nome", type = "string" },
        { name = "meta_alfabetizacao", type = "double" },
        { name = "taxa_alfabetizacao", type = "double" },
        { name = "percentual_participacao", type = "double" },
        { name = "nivel_alfabetizacao", type = "int" },
      ]
    }

    meta_vs_resultado = {
      location = "integracao/meta_vs_resultado"
      columns  = local.colunas_meta_vs_resultado
    }

    quarentena = {
      location = "quarentena"
      columns = concat(
        local.colunas_meta_vs_resultado,
        [
          { name = "motivo_quarentena", type = "string" },
          { name = "origem", type = "string" },
        ],
      )
    }
  }
}

resource "aws_glue_catalog_table" "silver" {
  for_each = local.tabelas_silver

  name          = each.key
  database_name = aws_glue_catalog_database.camada["silver"].name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_destino}/silver/${each.value.location}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = each.value.columns

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }

  // As tabelas descrevem a saída do Job. Declarar a dependência garante
  // que o Job exista antes — não que já tenha rodado, o que é ação e não
  // estado.
  depends_on = [aws_glue_job.silver]
}
