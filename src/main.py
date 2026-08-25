from pathlib import Path

from src.cloud.s3 import upload_file
from src.governance.observabilidade import MonitorExecucao
from src.ingestion.extract import extract_table
from src.ingestion.writer import save_parquet

TABLES = [
    "uf",
    "municipio",
    "alunos",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "dicionario",
]

def main():
    monitor = MonitorExecucao(etapa="ingestao_bronze")

    print("=" * 60)
    print("INICIANDO EXTRAÇÃO BASE DOS DADOS")
    print("=" * 60)

    try:
        for table in TABLES:
            print("-" * 60)

            df = extract_table(table)

            file_path = save_parquet(df, table)

            upload_file(file_path)

            monitor.registrar_tabela(
                tabela=table,
                linhas=len(df),
                bytes_arquivo=Path(file_path).stat().st_size,
            )

    except Exception as erro:
        monitor.finalizar(sucesso=False, erro=str(erro))
        raise

    monitor.finalizar(sucesso=True)

    print("=" * 60)
    print("PROCESSO FINALIZADO")
    print("=" * 60)


if __name__ == "__main__":
    main()