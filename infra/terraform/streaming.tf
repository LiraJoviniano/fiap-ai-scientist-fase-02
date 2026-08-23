// ---------------------------------------------------------------------------
// Streaming — processamento Bronze -> Silver -> Gold
//
// Este fluxo é complementar ao pipeline batch existente.
// Não altera o workflow principal Bronze -> Silver -> Qualidade -> Gold.
// ---------------------------------------------------------------------------

resource "aws_s3_object" "script_streaming_silver" {
  bucket = var.bucket
  key    = "scripts/streaming_silver.py"
  source = var.caminho_script_streaming_silver
  etag   = filemd5(var.caminho_script_streaming_silver)
}

resource "aws_glue_job" "streaming_silver" {
  name        = "${var.prefixo}_job_streaming_silver"
  description = "Streaming - transforma eventos da Bronze em Silver"
  role_arn    = var.role_glue

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 15

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket}/${aws_s3_object.script_streaming_silver.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--BUCKET_DESTINO" = var.bucket
    "--ENV"            = var.ambiente

    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  tags = merge(local.tags_comuns, {
    Layer     = "silver"
    Component = "streaming"
  })
}

resource "aws_s3_object" "script_streaming_gold" {
  bucket = var.bucket
  key    = "scripts/streaming_gold.py"
  source = var.caminho_script_streaming_gold
  etag   = filemd5(var.caminho_script_streaming_gold)
}

resource "aws_glue_job" "streaming_gold" {
  name        = "${var.prefixo}_job_streaming_gold"
  description = "Streaming - consolida o estado atual dos indicadores"
  role_arn    = var.role_glue

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 15

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket}/${aws_s3_object.script_streaming_gold.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--BUCKET_DESTINO" = var.bucket
    "--ENV"            = var.ambiente

    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  depends_on = [
    aws_glue_job.streaming_silver
  ]

  tags = merge(local.tags_comuns, {
    Layer     = "gold"
    Component = "streaming"
  })
}