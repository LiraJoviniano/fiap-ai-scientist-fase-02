"""Inventário, estimativa e lifecycle FinOps para o data lake no S3."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from src.config.settings import settings

BYTES_POR_GIB = 1024**3
TARIFAS_USD_GB_MES = {
    "STANDARD": 0.023,
    "STANDARD_IA": 0.0125,
    "GLACIER_IR": 0.004,
}
TARIFAS_SERVICOS = {
    "glue_dpu_hora": 0.44,
    "athena_tib": 5.00,
    "kinesis_shard_hora": 0.015,
    "kinesis_put_milhao": 0.014,
    "lambda_requisicoes_milhao": 0.20,
    "lambda_gb_segundo": 0.0000166667,
    "bigquery_tib": 6.25,
}

IDS_REGRAS_FINOPS = {
    "finops-bronze-transicao",
    "finops-abortar-multipart-incompleto",
}


def estimar_armazenamento_usd(bytes_total: int, tarifa_gb_mes: float) -> float:
    """Converte bytes em GiB e calcula uma estimativa mensal simples."""

    return round((bytes_total / BYTES_POR_GIB) * tarifa_gb_mes, 6)


def estimar_servicos(uso: dict[str, float]) -> dict[str, float]:
    """Estima o custo mensal a partir das premissas de uso informadas."""

    custos = {
        "glue": uso["glue_dpu_horas"] * TARIFAS_SERVICOS["glue_dpu_hora"],
        "athena": (uso["athena_gb"] / 1024) * TARIFAS_SERVICOS["athena_tib"],
        "kinesis": (
            uso["kinesis_shard_horas"] * TARIFAS_SERVICOS["kinesis_shard_hora"]
            + uso["kinesis_put_milhao"] * TARIFAS_SERVICOS["kinesis_put_milhao"]
        ),
        "lambda": (
            uso["lambda_requisicoes_milhao"]
            * TARIFAS_SERVICOS["lambda_requisicoes_milhao"]
            + uso["lambda_gb_segundos"] * TARIFAS_SERVICOS["lambda_gb_segundo"]
        ),
        "bigquery": max(uso["bigquery_tib"] - 1, 0)
        * TARIFAS_SERVICOS["bigquery_tib"],
    }
    return {servico: round(valor, 6) for servico, valor in custos.items()}


def camada_da_chave(chave: str) -> str:
    """Retorna o primeiro diretório da chave S3: bronze, silver ou gold."""

    return chave.split("/", 1)[0] if "/" in chave else "raiz"


def inventariar(s3, bucket: str, prefixo: str = "") -> dict:
    """Conta objetos e bytes sem baixar nem abrir os arquivos do bucket."""

    por_camada = defaultdict(lambda: {"objetos": 0, "bytes": 0})
    por_classe = defaultdict(lambda: {"objetos": 0, "bytes": 0})
    total_objetos = 0
    total_bytes = 0
    paginador = s3.get_paginator("list_objects_v2")
    for pagina in paginador.paginate(Bucket=bucket, Prefix=prefixo):
        for objeto in pagina.get("Contents", []):
            chave = objeto["Key"]
            tamanho = objeto["Size"]
            classe = objeto.get("StorageClass", "STANDARD")
            total_objetos += 1
            total_bytes += tamanho
            por_camada[camada_da_chave(chave)]["objetos"] += 1
            por_camada[camada_da_chave(chave)]["bytes"] += tamanho
            por_classe[classe]["objetos"] += 1
            por_classe[classe]["bytes"] += tamanho
    return {
        "total_objetos": total_objetos,
        "total_bytes": total_bytes,
        "por_camada": dict(por_camada),
        "por_classe": dict(por_classe),
    }


def configuracao_lifecycle(prefixo: str, dias_ia: int, dias_glacier: int) -> dict:
    """Monta as duas regras FinOps, mas ainda não altera o bucket."""

    if not 0 < dias_ia < dias_glacier:
        raise ValueError("Os dias devem obedecer 0 < IA < Glacier")
    return {
        "Rules": [
            {
                "ID": "finops-bronze-transicao",
                "Status": "Enabled",
                "Filter": {
                    "And": {
                        "Prefix": prefixo,
                        "ObjectSizeGreaterThan": 131072,
                    }
                },
                "Transitions": [
                    {"Days": dias_ia, "StorageClass": "STANDARD_IA"},
                    {"Days": dias_glacier, "StorageClass": "GLACIER_IR"},
                ],
            },
            {
                "ID": "finops-abortar-multipart-incompleto",
                "Status": "Enabled",
                "Filter": {"Prefix": prefixo},
                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
            },
        ]
    }


def combinar_regras_lifecycle(
    regras_atuais: list[dict], regras_finops: list[dict]
) -> list[dict]:
    """Preserva regras de terceiros e substitui somente as regras deste projeto."""

    regras_preservadas = [
        regra for regra in regras_atuais if regra.get("ID") not in IDS_REGRAS_FINOPS
    ]
    return regras_preservadas + regras_finops


def consultar_lifecycle(s3, bucket: str) -> list[dict]:
    """Consulta o lifecycle; bucket sem regras é um estado válido e retorna []."""

    try:
        return s3.get_bucket_lifecycle_configuration(Bucket=bucket).get("Rules", [])
    except ClientError as erro:
        codigo = erro.response.get("Error", {}).get("Code")
        if codigo in {"NoSuchLifecycleConfiguration", "NoSuchLifecycle"}:
            return []
        raise


def mensagem_erro_aws(erro: ClientError) -> str:
    """Traduz os erros AWS mais comuns para uma orientação objetiva."""

    codigo = erro.response.get("Error", {}).get("Code", "ClientError")
    mensagens = {
        "ExpiredToken": "A credencial do AWS Academy expirou. Atualize o arquivo credentials.",
        "InvalidToken": "O token AWS é inválido. Copie novamente as credenciais do Learner Lab.",
        "AccessDenied": "A credencial atual não tem permissão para acessar esse bucket.",
        "NoSuchBucket": "O bucket configurado no .env não existe.",
    }
    return mensagens.get(codigo, f"Erro da AWS ({codigo}). Consulte os detalhes da operação.")


def gerar_relatorio(bucket: str, regiao: str, prefixo: str = "") -> tuple[dict, Path]:
    """Consulta o S3 e grava uma versão técnica (JSON) e uma legível (Markdown)."""

    s3 = boto3.client("s3", region_name=regiao)
    inventario = inventariar(s3, bucket, prefixo)
    total = inventario["total_bytes"]
    custos_s3 = {
        classe: estimar_armazenamento_usd(
            dados["bytes"], TARIFAS_USD_GB_MES.get(classe, TARIFAS_USD_GB_MES["STANDARD"])
        )
        for classe, dados in inventario["por_classe"].items()
    }
    uso_servicos = {
        "glue_dpu_horas": settings.FINOPS_GLUE_DPU_HORAS_MES,
        "athena_gb": settings.FINOPS_ATHENA_GB_MES,
        "kinesis_shard_horas": settings.FINOPS_KINESIS_SHARD_HORAS_MES,
        "kinesis_put_milhao": settings.FINOPS_KINESIS_PUT_MILHAO_MES,
        "lambda_requisicoes_milhao": settings.FINOPS_LAMBDA_REQUISICOES_MILHAO_MES,
        "lambda_gb_segundos": settings.FINOPS_LAMBDA_GB_SEGUNDOS_MES,
        "bigquery_tib": settings.FINOPS_BIGQUERY_TIB_MES,
    }
    custos_servicos = estimar_servicos(uso_servicos)
    custo_total = sum(custos_s3.values()) + sum(custos_servicos.values())
    relatorio = {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "bucket": bucket,
        "regiao": regiao,
        "prefixo": prefixo,
        "inventario": inventario,
        "lifecycle_atual": consultar_lifecycle(s3, bucket),
        "estimativa_s3_usd_mes": custos_s3,
        "uso_mensal_informado": uso_servicos,
        "estimativa_servicos_usd_mes": custos_servicos,
        "estimativa_total_usd_mes": round(custo_total, 6),
        "premissas": {
            "tarifas_usd_gb_mes": TARIFAS_USD_GB_MES,
            "inclui": "armazenamento dos objetos inventariados",
            "nao_inclui": "S3 requests, recuperação, permanência mínima, transferência e impostos",
        },
    }
    pasta = Path("reports/finops")
    pasta.mkdir(parents=True, exist_ok=True)
    json_path = pasta / "relatorio-finops.json"
    json_path.write_text(json.dumps(relatorio, indent=2), encoding="utf-8")

    mib = total / 1024**2
    md = [
        "# Relatório FinOps do S3",
        "",
        f"- Bucket: `{bucket}`",
        f"- Objetos: **{inventario['total_objetos']}**",
        f"- Volume: **{mib:.3f} MiB**",
        f"- Estimativa total: **US$ {relatorio['estimativa_total_usd_mes']:.6f}/mês**",
        "",
        "## Volume por camada",
        "",
        "| Camada | Objetos | MiB |",
        "|---|---:|---:|",
    ]
    for camada, dados in sorted(inventario["por_camada"].items()):
        md.append(f"| {camada} | {dados['objetos']} | {dados['bytes'] / 1024**2:.3f} |")
    md += [
        "",
        "## Estimativa por serviço",
        "",
        "| Serviço | Custo mensal (USD) |",
        "|---|---:|",
        f"| S3 | {sum(custos_s3.values()):.6f} |",
        *[
            f"| {servico.title()} | {custo:.6f} |"
            for servico, custo in custos_servicos.items()
        ],
        f"| **Total** | **{custo_total:.6f}** |",
        "",
        "## Limites da estimativa",
        "",
        "Valores ilustrativos para comparação arquitetural. O custo real deve ser confirmado no AWS Pricing Calculator/Cost Explorer e inclui outras dimensões.",
    ]
    md_path = pasta / "relatorio-finops.md"
    md_path.write_text("\n".join(md) + "\n", encoding="utf-8")
    return relatorio, md_path


def main() -> None:
    parser = argparse.ArgumentParser(description="FinOps do data lake S3")
    parser.add_argument("acao", choices=["relatorio", "verificar", "aplicar-lifecycle"])
    parser.add_argument("--bucket", default=settings.AWS_BUCKET)
    parser.add_argument("--regiao", default=settings.AWS_REGION or "us-east-1")
    parser.add_argument("--prefixo")
    parser.add_argument("--aplicar", action="store_true", help="Confirma alteração no S3")
    args = parser.parse_args()
    if not args.bucket:
        parser.error("AWS_BUCKET não configurado no .env")

    s3 = boto3.client("s3", region_name=args.regiao)
    try:
        if args.acao == "relatorio":
            prefixo = (
                args.prefixo
                if args.prefixo is not None
                else settings.FINOPS_RELATORIO_PREFIX
            )
            relatorio, caminho = gerar_relatorio(args.bucket, args.regiao, prefixo)
            print(f"Objetos: {relatorio['inventario']['total_objetos']}")
            print(f"Bytes: {relatorio['inventario']['total_bytes']}")
            print(f"Relatório: {caminho}")
        elif args.acao == "verificar":
            regras = consultar_lifecycle(s3, args.bucket)
            print(json.dumps(regras, indent=2, default=str))
        elif not args.aplicar:
            parser.error("Use --aplicar para confirmar a alteração do lifecycle")
        else:
            prefixo = args.prefixo or settings.FINOPS_LIFECYCLE_PREFIX
            regras_finops = configuracao_lifecycle(
                prefixo,
                settings.FINOPS_TRANSICAO_IA_DIAS,
                settings.FINOPS_TRANSICAO_GLACIER_DIAS,
            )["Rules"]
            regras_atuais = consultar_lifecycle(s3, args.bucket)
            regras_finais = combinar_regras_lifecycle(regras_atuais, regras_finops)
            s3.put_bucket_lifecycle_configuration(
                Bucket=args.bucket,
                LifecycleConfiguration={"Rules": regras_finais},
            )
            print(f"Lifecycle FinOps aplicado em s3://{args.bucket}/{prefixo}")
            print(f"Regras de terceiros preservadas: {len(regras_finais) - len(regras_finops)}")
    except ClientError as erro:
        print(f"ERRO: {mensagem_erro_aws(erro)}")
        raise SystemExit(1) from None
    except NoCredentialsError:
        print("ERRO: credenciais AWS não encontradas. Atualize o Learner Lab.")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
