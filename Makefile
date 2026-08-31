.PHONY: setup install run test-bq test-aws list-buckets freeze clean help \
        check-terraform tf-init tf-plan tf-apply tf-destroy \
        workflow crawler silver silver-completa \
        streaming streaming-producer streaming-silver streaming-gold \
        streaming-status streaming-ls lambda-package

PYTHON = python

# Sobrescreva na linha de comando quando necessário:
#     make tf-apply PREFIXO=teste
PREFIXO ?= alfabetizacao
BUCKET ?= fiap-ai-scientist-fase-02
REGION ?= us-east-1

LAMBDA_HANDLER = src/ingestion/streaming/lambda_handler.py
LAMBDA_BUILD_DIR = lambda_package
LAMBDA_ZIP = lambda_function.zip
# Versao presa em 20.0.0 de proposito: e a ultima que ainda publica wheel
# manylinux2014, compativel com o glibc do runtime Python 3.11 da Lambda
# (Amazon Linux 2). Versoes mais novas do PyArrow (25.x, usado localmente
# no requirements.txt) só publicam wheel manylinux_2_28, que quebra na
# Lambda com erro de GLIBC. Nao sincronizar essa versao com o requirements.txt.
LAMBDA_PYARROW_VERSION = 20.0.0
LAMBDA_PYTHON_VERSION = 3.11

setup:
	$(PYTHON) -m pip install --upgrade pip
	pip install -r requirements.txt

install:
	pip install -r requirements.txt

run:
	$(PYTHON) -m src.main

test-bq:
	$(PYTHON) src/teste_bigquery.py

test-aws:
	aws sts get-caller-identity

list-buckets:
	aws s3 ls

freeze:
	pip freeze > requirements.txt

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Infraestrutura — Terraform ---------------------------------------
#
# Toda a infraestrutura é declarada em infra/terraform: databases,
# crawler, job, upload do script e as tabelas da Silver.
 
# Verifica, não instala: make não é gerenciador de pacotes, e instalar
# software na máquina de quem executa surpreende — além de exigir
# elevação em máquina corporativa.
check-terraform:
	@terraform version > /dev/null 2>&1 && terraform version || \
	  (echo "Terraform nao encontrado."; \
	   echo "  Windows: winget install HashiCorp.Terraform"; \
	   echo "  macOS:   brew install terraform"; \
	   echo "  Linux:   https://terraform.io/downloads"; \
	   exit 1)
 
tf-init: check-terraform
	cd infra/terraform && terraform init -input=false
 
tf-plan: check-terraform
	cd infra/terraform && terraform plan -var="prefixo=$(PREFIXO)"
 
tf-apply: check-terraform
	cd infra/terraform && terraform apply -var="prefixo=$(PREFIXO)"
 
tf-destroy: check-terraform
	cd infra/terraform && terraform destroy -var="prefixo=$(PREFIXO)"
 
# Execução ---------------------------------------------------------
#
# O Terraform declara estado; executar é ação, e fica nos scripts.
 
# Caminho principal: o Workflow encadeia crawler, Silver e qualidade
# dentro da AWS. Uma etapa so dispara se a anterior teve sucesso.
workflow:
	bash infra/executar_workflow.sh
 
# Etapas isoladas, para depurar sem rodar o fluxo inteiro
crawler:
	bash infra/executar_crawler.sh
 
silver:
	bash infra/executar_job_silver.sh
 
# Infraestrutura mais execução orquestrada, do zero ao relatório.
silver-completa: tf-apply workflow
	@echo "Camada Silver concluida e validada"

# Streaming — Kinesis + Lambda ---------------------------------------
#
# Fluxo complementar ao pipeline batch: producer -> Kinesis -> Lambda
# (grava a Bronze Streaming) -> Streaming Silver -> Streaming Gold.
# Nao depende do Workflow acima nem o altera.

# Gera lambda_function.zip a partir do handler real (src/ingestion/streaming/
# lambda_handler.py). Usa wheel pre-compilado para Linux (manylinux2014,
# compativel com o glibc do Amazon Linux 2 - ver LAMBDA_PYARROW_VERSION acima),
# independente do SO de quem executa. O zip e montado em Python (nao `zip`),
# porque nem todo Git Bash no Windows tem esse utilitario instalado.
lambda-package:
	rm -rf $(LAMBDA_BUILD_DIR) $(LAMBDA_ZIP)
	mkdir -p $(LAMBDA_BUILD_DIR)
	cp $(LAMBDA_HANDLER) $(LAMBDA_BUILD_DIR)/lambda_handler.py
	pip install pyarrow==$(LAMBDA_PYARROW_VERSION) \
	  --platform manylinux2014_x86_64 \
	  --python-version $(LAMBDA_PYTHON_VERSION) \
	  --only-binary=:all: \
	  --target $(LAMBDA_BUILD_DIR)
	python -c "import os, zipfile; zf = zipfile.ZipFile('$(LAMBDA_ZIP)', 'w', zipfile.ZIP_DEFLATED); [zf.write(os.path.join(r, f), os.path.relpath(os.path.join(r, f), '$(LAMBDA_BUILD_DIR)')) for r, d, fs in os.walk('$(LAMBDA_BUILD_DIR)') for f in fs if '.dist-info' not in r]; zf.close()"
	@echo "Pacote gerado: $(LAMBDA_ZIP)"

streaming-producer:
	$(PYTHON) -m src.ingestion.streaming.producer

streaming-silver:
	aws glue start-job-run \
	  --job-name $(PREFIXO)_job_streaming_silver \
	  --region $(REGION)

streaming-gold:
	aws glue start-job-run \
	  --job-name $(PREFIXO)_job_streaming_gold \
	  --region $(REGION)

streaming-status:
	aws glue get-job-runs \
	  --job-name $(PREFIXO)_job_streaming_silver \
	  --region $(REGION) --max-results 1 \
	  --query 'JobRuns[0].[Id,JobRunState,StartedOn,CompletedOn,ErrorMessage]' \
	  --output table
	aws glue get-job-runs \
	  --job-name $(PREFIXO)_job_streaming_gold \
	  --region $(REGION) --max-results 1 \
	  --query 'JobRuns[0].[Id,JobRunState,StartedOn,CompletedOn,ErrorMessage]' \
	  --output table

streaming-ls:
	aws s3 ls s3://$(BUCKET)/bronze/streaming/ --recursive --region $(REGION)
	aws s3 ls s3://$(BUCKET)/silver/streaming/ --recursive --region $(REGION)
	aws s3 ls s3://$(BUCKET)/gold/streaming/   --recursive --region $(REGION)

# Producer + Streaming Silver. A Streaming Gold dispara sozinha via
# trigger condicional quando a Silver terminar com sucesso.
streaming: streaming-producer streaming-silver
	@echo "Streaming disparado - producer + Silver (Gold dispara sozinha)"

help:
	@echo ""
	@echo "Comandos disponíveis:"
	@echo " make setup         -> Configura o ambiente"
	@echo " make install       -> Instala dependências"
	@echo " make run           -> Executa pipeline de ingestão"
	@echo " make test-bq       -> Testa conexão com BigQuery"
	@echo " make test-aws      -> Testa autenticação AWS"
	@echo " make list-buckets  -> Lista buckets S3"
	@echo " make freeze        -> Atualiza requirements.txt"
	@echo " make clean         -> Remove cache Python"
	@echo ""
	@echo "Infraestrutura (Terraform):"
	@echo " make tf-init         -> Inicializa o Terraform"
	@echo " make tf-plan         -> Mostra o que seria criado"
	@echo " make tf-apply        -> Cria a infraestrutura"
	@echo " make tf-destroy      -> Remove a infraestrutura"
	@echo ""
	@echo "Camada Silver:"
	@echo " make workflow        -> Executa o fluxo completo na AWS"
	@echo " make silver-completa -> Infra + workflow, do zero ao relatorio"
	@echo ""
	@echo "Etapas isoladas (depuracao):"
	@echo " make crawler         -> So o crawler da Bronze"
	@echo " make silver          -> So o Glue Job da Silver"
	@echo ""
	@echo "Streaming (Kinesis + Lambda):"
	@echo " make lambda-package     -> Gera lambda_function.zip a partir do handler real"
	@echo " make streaming          -> Producer + Streaming Silver + Streaming Gold"
	@echo " make streaming-producer -> So publica eventos no Kinesis"
	@echo " make streaming-silver   -> So o Glue Job da Streaming Silver"
	@echo " make streaming-gold     -> So o Glue Job da Streaming Gold"
	@echo " make streaming-status   -> Status das ultimas execucoes"
	@echo " make streaming-ls       -> Lista os arquivos gravados no S3"
	@echo ""