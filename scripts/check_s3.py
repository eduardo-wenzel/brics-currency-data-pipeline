import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def main():
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ModuleNotFoundError as exc:
        raise SystemExit("boto3 nao instalado no ambiente atual.") from exc

    bucket = os.getenv("AWS_S3_BUCKET", "").strip()
    region = (os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION") or "").strip()

    if not bucket:
        raise SystemExit("AWS_S3_BUCKET nao configurado no .env")

    client = boto3.client("s3", region_name=region or None)

    print(f"Bucket configurado: {bucket}")
    print(f"Regiao configurada: {region or 'nao informada'}")

    try:
        response = client.head_bucket(Bucket=bucket)
        print("HeadBucket: OK")
        bucket_region = (
            response.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("x-amz-bucket-region")
        )
        if bucket_region:
            print(f"Regiao retornada pela AWS: {bucket_region}")
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "desconhecido")
        headers = exc.response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
        bucket_region = headers.get("x-amz-bucket-region")
        print(f"HeadBucket: FALHOU ({error_code})")
        if bucket_region:
            print(f"Regiao retornada pela AWS: {bucket_region}")
        raise SystemExit(
            "O bucket nao foi encontrado para as credenciais/regiao atuais, ou o nome do bucket esta incorreto."
        ) from exc


if __name__ == "__main__":
    main()
