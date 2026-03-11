import io
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

LOCAL_RAW_DIR = Path("data/raw")
LOCAL_PROCESSED_DIR = Path("data/processed")


def storage_backend() -> str:
    return os.getenv("DATA_LAKE_BACKEND", "local").strip().lower()


def _s3_bucket() -> str:
    bucket = os.getenv("AWS_S3_BUCKET", "").strip()
    if not bucket:
        raise OSError("Variavel de ambiente AWS_S3_BUCKET nao configurada.")
    return bucket


def _s3_region() -> str:
    return (os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION") or "").strip()


def _s3_prefix() -> str:
    return os.getenv("AWS_S3_PREFIX", "brics-currency").strip().strip("/")


def _s3_layer_prefix(layer: str) -> str:
    default = f"{layer}/exchange_rates"
    env_name = f"S3_{layer.upper()}_PREFIX"
    return os.getenv(env_name, default).strip().strip("/")


def _s3_client():
    try:
        import boto3
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "boto3 nao instalado. Instale as dependencias para usar DATA_LAKE_BACKEND=s3."
        ) from exc

    return boto3.client("s3", region_name=_s3_region() or None)


def _build_s3_key(layer: str, timestamp: str, extension: str, reference_date: Any = None) -> str:
    parts = [_s3_prefix(), _s3_layer_prefix(layer)]
    if reference_date is not None:
        reference = str(reference_date)
        parts.extend(
            [
                f"year={reference[0:4]}",
                f"month={reference[5:7]}",
                f"day={reference[8:10]}",
            ]
        )
    filename = f"brics_rates_{timestamp}.{extension}"
    return "/".join(part for part in [*parts, filename] if part)


def _to_s3_uri(key: str) -> str:
    return f"s3://{_s3_bucket()}/{key}"


def _raise_s3_error(exc: Exception, action: str, key: str | None = None):
    error_code = None
    try:
        error_code = exc.response.get("Error", {}).get("Code")
    except AttributeError:
        error_code = None

    bucket = _s3_bucket()
    region = _s3_region() or "regiao-nao-configurada"
    target = f" bucket '{bucket}'"
    if key:
        target += f", chave '{key}'"

    if error_code == "NoSuchBucket":
        raise RuntimeError(
            f"Falha ao {action} no S3:{target}. O bucket nao existe na conta atual ou nao esta na regiao '{region}'."
        ) from exc

    if error_code in {"AccessDenied", "AllAccessDisabled"}:
        raise RuntimeError(
            f"Falha ao {action} no S3:{target}. A conta atual nao tem permissao para acessar esse bucket."
        ) from exc

    if error_code == "NoSuchKey":
        raise FileNotFoundError(
            f"Falha ao {action} no S3:{target}. O objeto informado nao foi encontrado."
        ) from exc

    raise RuntimeError(f"Falha ao {action} no S3:{target}. Erro AWS: {error_code or type(exc).__name__}.") from exc


def _put_object(*, key: str, body: bytes, content_type: str):
    try:
        _s3_client().put_object(
            Bucket=_s3_bucket(),
            Key=key,
            Body=body,
            ContentType=content_type,
        )
    except Exception as exc:
        _raise_s3_error(exc, action="gravar objeto", key=key)


def _list_objects(prefix: str):
    try:
        response = _s3_client().list_objects_v2(Bucket=_s3_bucket(), Prefix=prefix)
    except Exception as exc:
        _raise_s3_error(exc, action="listar objetos", key=prefix)
    return response.get("Contents", [])


def _get_object_bytes(key: str) -> bytes:
    try:
        response = _s3_client().get_object(Bucket=_s3_bucket(), Key=key)
    except Exception as exc:
        _raise_s3_error(exc, action="ler objeto", key=key)
    return response["Body"].read()


def save_raw_data(data: dict):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if storage_backend() == "s3":
        key = _build_s3_key("bronze", timestamp, "json")
        payload = json.dumps(data, indent=4).encode("utf-8")
        _put_object(key=key, body=payload, content_type="application/json")
        return _to_s3_uri(key)

    LOCAL_RAW_DIR.mkdir(parents=True, exist_ok=True)
    file_path = LOCAL_RAW_DIR / f"brics_rates_{timestamp}.json"
    with file_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)
    return file_path


def get_latest_raw_file():
    if storage_backend() == "s3":
        prefix = f"{_s3_prefix()}/{_s3_layer_prefix('bronze')}/"
        contents = _list_objects(prefix)
        if not contents:
            raise FileNotFoundError("Nenhum arquivo raw encontrado no bucket S3 configurado.")
        return max(contents, key=lambda item: item["LastModified"])["Key"]

    files = list(LOCAL_RAW_DIR.glob("*.json"))
    if not files:
        raise FileNotFoundError("Nenhum arquivo raw encontrado.")
    return max(files, key=lambda file: file.stat().st_mtime)


def read_raw_data(file_ref) -> dict:
    if isinstance(file_ref, str) and file_ref.startswith("s3://"):
        file_ref = file_ref.removeprefix(f"s3://{_s3_bucket()}/")

    if storage_backend() == "s3" or isinstance(file_ref, str):
        return json.loads(_get_object_bytes(file_ref).decode("utf-8"))

    with file_ref.open(encoding="utf-8") as file:
        return json.load(file)


def save_processed_data(df: pd.DataFrame):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    reference_date = None if df.empty else df["reference_date"].iloc[0]

    if storage_backend() == "s3":
        key = _build_s3_key("silver", timestamp, "parquet", reference_date=reference_date)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        _put_object(key=key, body=buffer.getvalue(), content_type="application/octet-stream")
        return _to_s3_uri(key)

    LOCAL_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    output_file = LOCAL_PROCESSED_DIR / f"brics_rates_{timestamp}.parquet"
    df.to_parquet(output_file, index=False)
    return output_file
