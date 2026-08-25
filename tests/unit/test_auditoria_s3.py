from src.governance import auditoria_s3


class S3Falso:
    def get_bucket_encryption(self, **_kwargs):
        return {"ServerSideEncryptionConfiguration": {}}

    def get_public_access_block(self, **_kwargs):
        return {
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            }
        }

    def get_bucket_versioning(self, **_kwargs):
        return {}

    def get_bucket_lifecycle_configuration(self, **_kwargs):
        return {"Rules": [{"ID": "finops-bronze-transicao"}]}


def test_auditoria_reconhece_controles_configurados(monkeypatch):
    monkeypatch.setattr(auditoria_s3.boto3, "client", lambda *_args, **_kwargs: S3Falso())

    relatorio = auditoria_s3.auditar_bucket("bucket-teste", "us-east-1")
    resultados = {item["controle"]: item for item in relatorio["resultado"]}

    assert resultados["criptografia_em_repouso"]["status"] == "CONFORME"
    assert resultados["bloqueio_acesso_publico"]["status"] == "CONFORME"
    assert resultados["versionamento"]["status"] == "INFORMATIVO"
    assert resultados["lifecycle_finops"]["status"] == "CONFORME"
    assert resultados["lifecycle_finops"]["detalhe"] == "1 regra(s) encontrada(s)"
