// ---------------------------------------------------------------------------
// Camada Gold — Job, tabelas e encadeamento
//
// A Gold agrega; a limpeza já aconteceu na Silver. Três datasets prontos
// para consumo: a série do indicador, a trajetória até a meta de 2030 e a
// tabela de features para modelagem.
//
// O schema é declarado, como na Silver: o que esta camada entrega é
// decisão de desenho, não descoberta.
//
// As definições espelham ESQUEMA_GOLD em src/transformation/gold.py. Se
// uma mudar, a outra precisa mudar junto.
// ---------------------------------------------------------------------------

resource "aws_s3_object" "script_gold" {
  bucket = var.bucket
  key    = "scripts/gold.py"
  source = var.caminho_script_gold
  etag   = filemd5(var.caminho_script_gold)
}

resource "aws_glue_job" "gold" {
  name        = "${var.prefixo}_job_gold"
  description = "Camada Gold - indicadores, trajetoria ate 2030 e features"
  role_arn    = var.role_glue

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  timeout = 20

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket}/${aws_s3_object.script_gold.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--BUCKET_DESTINO"  = local.bucket_destino
    "--DATABASE_SILVER" = aws_glue_catalog_database.camada["silver"].name
    "--ENV"             = var.ambiente

    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"

    // Lê a Silver pelo Catalog com spark.sql e create_dynamic_frame
    "--enable-glue-datacatalog" = "true"

    "--job-bookmark-option" = "job-bookmark-disable"
  }

  tags = merge(local.tags_comuns, { Layer = "gold" })

  depends_on = [aws_glue_catalog_table.silver]
}

// ---------------------------------------------------------------------------
// Tabelas da Gold
// ---------------------------------------------------------------------------

locals {
  colunas_nivel_gold = [
    for i in range(9) : {
      name = "proporcao_aluno_nivel_${i}"
      type = "double"
    }
  ]

  tabelas_gold = {
    // Série completa, todas as redes. A meta só existe para a rede
    // Municipal no nível do município — nas demais fica nula, o que é
    // mais honesto que omitir a linha.
    indicador_municipio = {
      location = "indicadores/indicador_municipio"
      columns = concat(
        [
          { name = "ano", type = "int" },
          { name = "id_municipio", type = "string" },
          { name = "sigla_uf", type = "string" },
          { name = "regiao", type = "string" },
          { name = "rede_codigo", type = "string" },
          { name = "rede_nome", type = "string" },
          { name = "taxa_alfabetizacao", type = "double" },
          { name = "media_portugues", type = "double" },
          { name = "meta_alfabetizacao", type = "double" },
          { name = "distancia_meta", type = "double" },
          { name = "atingiu_meta", type = "boolean" },
          { name = "situacao_meta", type = "string" },
          { name = "tem_distribuicao_nivel", type = "boolean" },
        ],
        local.colunas_nivel_gold,
      )
    }

    // Formato longo, uma linha por município e ano — é o que um gráfico
    // de linha consome, diferente da trajetória, que traz os anos em
    // colunas. Atende ao item "evolução temporal do indicador".
    evolucao_temporal = {
      location = "indicadores/evolucao_temporal"
      columns = [
        { name = "id_municipio", type = "string" },
        { name = "sigla_uf", type = "string" },
        { name = "regiao", type = "string" },
        { name = "ano", type = "int" },
        { name = "taxa_alfabetizacao", type = "double" },
        { name = "taxa_ano_anterior", type = "double" },
        { name = "variacao_absoluta", type = "double" },
        // Nula quando a taxa anterior é zero: variação percentual sobre
        // zero é indefinida, não infinita.
        { name = "variacao_percentual", type = "double" },
        { name = "taxa_ano_base", type = "double" },
        { name = "variacao_acumulada", type = "double" },
        { name = "meta_alfabetizacao", type = "double" },
        { name = "distancia_meta", type = "double" },
        { name = "situacao_meta", type = "string" },
      ]
    }

    // Uma linha por município. O índice de trajetória compara o ritmo
    // observado com o necessário para alcançar 80% em 2030: acima de 1,
    // o município avança mais rápido do que precisa.
    trajetoria_meta_2030 = {
      location = "indicadores/trajetoria_meta_2030"
      columns = [
        { name = "id_municipio", type = "string" },
        { name = "sigla_uf", type = "string" },
        { name = "regiao", type = "string" },
        { name = "taxa_2023", type = "double" },
        { name = "taxa_2024", type = "double" },
        { name = "variacao_anual", type = "double" },
        { name = "meta_2024", type = "double" },
        { name = "atingiu_meta_2024", type = "boolean" },
        { name = "distancia_meta_2030", type = "double" },
        { name = "ritmo_necessario", type = "double" },
        // Nulo quando o município já alcançou os 80%: o ritmo necessário
        // seria zero ou negativo, e o índice ficaria indefinido.
        { name = "indice_trajetoria", type = "double" },
        { name = "classificacao_trajetoria", type = "string" },
        { name = "elegivel_meta", type = "boolean" },
      ]
    }

    // Insumo de quem for modelar. Dois alvos, para que a escolha entre
    // regressão e classificação seja de quem modela.
    features_municipio = {
      location = "analiticos/features_municipio"
      columns = [
        { name = "id_municipio", type = "string" },
        { name = "sigla_uf", type = "string" },
        { name = "regiao", type = "string" },
        { name = "alvo_taxa_2024", type = "double" },
        { name = "alvo_atingiu_meta", type = "boolean" },
        // Sem meta publicada não há classe a prever: o município serve
        // para predição de taxa, não para risco de não atingimento.
        { name = "elegivel_meta", type = "boolean" },
        { name = "taxa_2023", type = "double" },
        { name = "variacao_anual", type = "double" },
        { name = "total_escolas", type = "int" },
        { name = "total_matriculas", type = "int" },
        { name = "alunos_por_docente", type = "double" },
        { name = "alunos_por_turma", type = "double" },
        { name = "pct_matricula_integral", type = "double" },
        { name = "pct_matricula_biblioteca", type = "double" },
        { name = "pct_matricula_lab_informatica", type = "double" },
        { name = "pct_matricula_banda_larga", type = "double" },
        { name = "pct_matricula_agua_adequada", type = "double" },
        { name = "pct_matricula_energia_publica", type = "double" },
        { name = "pct_matricula_esgoto_adequado", type = "double" },
        { name = "pct_matricula_alimentacao", type = "double" },
        { name = "indice_infraestrutura", type = "double" },
        { name = "pct_matricula_rural", type = "double" },
        { name = "pct_matricula_transporte", type = "double" },
        { name = "pct_escolas_urbanas", type = "double" },
      ]
    }
  }
}

resource "aws_glue_catalog_table" "gold" {
  for_each = local.tabelas_gold

  name          = each.key
  database_name = aws_glue_catalog_database.camada["gold"].name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${local.bucket_destino}/gold/${each.value.location}/"
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

  depends_on = [aws_glue_job.gold]
}

// ---------------------------------------------------------------------------
// Encadeamento
// ---------------------------------------------------------------------------

// A Gold só é gerada se a qualidade da Silver passar. O job de qualidade
// falha quando uma regra bloqueante reprova, e o gatilho condicional não
// dispara — o portão entre camadas é comportamento da AWS, não disciplina
// de quem executa.
resource "aws_glue_trigger" "qualidade_para_gold" {
  name          = "${var.prefixo}_trigger_gold"
  description   = "Gera a Gold depois de a Silver ser aprovada"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.silver.name

  predicate {
    conditions {
      job_name = aws_glue_job.qualidade.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.gold.name
  }

  tags = local.tags_comuns
}
