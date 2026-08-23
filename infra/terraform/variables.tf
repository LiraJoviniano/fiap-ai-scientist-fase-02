// Variáveis equivalentes às do script bash, com os mesmos padrões.
// Sobrescreva em terraform.tfvars ou pela linha de comando:
//   terraform apply -var="prefixo=fiap"

variable "prefixo" {
  description = "Prefixo dos databases. Evita colisão em conta compartilhada."
  type        = string
  default     = "alfabetizacao"

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]*$", var.prefixo))
    error_message = "O prefixo deve ser minúsculo, começar com letra e conter apenas letras, números e underscore."
  }
}

variable "bucket" {
  description = "Bucket S3 do data lake."
  type        = string
  default     = "fiap-ai-scientist-fase-02"
}

variable "regiao" {
  description = "Região AWS. O AWS Academy libera apenas us-east-1."
  type        = string
  default     = "us-east-1"
}

variable "role_glue" {
  description = "Role assumida pelo Crawler e pelos Glue Jobs. No AWS Academy nao e possivel criar roles; a LabRole ja vem provisionada."
  type        = string
  default     = "LabRole"
}

variable "caminho_script_silver" {
  description = "Caminho local do script PySpark da camada Silver, relativo a infra/terraform."
  type        = string
  default     = "../../src/transformation/silver.py"
}

variable "ambiente" {
  description = "Ambiente de execucao do Job: dev ou prod."
  type        = string
  default     = "prod"

  validation {
    condition     = contains(["dev", "prod"], var.ambiente)
    error_message = "O ambiente deve ser dev ou prod."
  }
}

variable "fonte_bronze" {
  description = "Origem de leitura da Bronze: catalog (via Glue Catalog) ou s3 (Parquet direto)."
  type        = string
  default     = "catalog"

  validation {
    condition     = contains(["catalog", "s3"], var.fonte_bronze)
    error_message = "A fonte deve ser catalog ou s3."
  }
}

variable "bucket_origem" {
  description = "Bucket de leitura da camada Bronze. Por padrao, o mesmo do data lake."
  type        = string
  default     = ""
}

variable "bucket_destino" {
  description = "Bucket de escrita da camada Silver. Por padrao, o mesmo do data lake."
  type        = string
  default     = ""
}

variable "caminho_script_qualidade" {
  description = "Caminho local do script Python Shell de qualidade, relativo a infra/terraform."
  type        = string
  default     = "../../src/transformation/qualidade_silver.py"
}

variable "caminho_script_gold" {
  description = "Caminho local do script PySpark da camada Gold, relativo a infra/terraform."
  type        = string
  default     = "../../src/transformation/gold.py"
}

variable "kinesis_stream_name" {
  description = "Nome do Kinesis Data Stream do streaming."
  type        = string
  default     = "alfabetizacao-events-dev"
}

variable "lambda_streaming_name" {
  description = "Nome da Lambda consumidora do streaming."
  type        = string
  default     = "alfabetizacao-streaming-lambda"
}


variable "caminho_lambda_zip" {
  description = "Pacote ZIP da Lambda de streaming."
  type        = string
  default     = "../../lambda_function.zip"
}

variable "caminho_script_streaming_silver" {
  description = "Caminho local do script PySpark da Silver de streaming."
  type        = string
  default     = "../../src/transformation/streaming_silver.py"
}

variable "caminho_script_streaming_gold" {
  description = "Caminho local do script PySpark da Gold de streaming."
  type        = string
  default     = "../../src/transformation/streaming_gold.py"
}