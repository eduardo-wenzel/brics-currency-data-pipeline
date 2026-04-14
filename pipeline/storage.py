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
LOCAL_GOLD_DIR = Path("data/gold")


def storage_backend() -> str:
    """Return the configured storage backend name."""
    return os.getenv("DATA_LAKE_BACKEND", "local").strip().lower()


def _s3_bucket() -> str:
    """Return the configured S3 bucket name or fail fast."""
    bucket = os.getenv("AWS_S3_BUCKET", "").strip()
    if not bucket:
        raise OSError("Variavel de ambiente AWS_S3_BUCKET nao configurada.")
    return bucket


def _s3_region() -> str:
    """Return the configured AWS region for S3 operations."""
    return (os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION") or "").strip()


def _s3_prefix() -> str:
    """Return the base S3 prefix used by the project."""
    return os.getenv("AWS_S3_PREFIX", "brics-currency").strip().strip("/")


def _s3_layer_prefix(layer: str) -> str:
    """Return the prefix for a specific medallion layer."""
    default = f"{layer}/exchange_rates"
    env_name = f"S3_{layer.upper()}_PREFIX"
    return os.getenv(env_name, default).strip().strip("/")


def _s3_client():
    """Create an S3 client only when the backend requires it."""
    try:
        import boto3
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "boto3 nao instalado. Instale as dependencias para usar DATA_LAKE_BACKEND=s3."
        ) from exc

    return boto3.client("s3", region_name=_s3_region() or None)


def _build_s3_key(layer: str, timestamp: str, extension: str, reference_date: Any = None) -> str:
    """Build the object key for a layer asset in S3."""
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
    """Convert an object key into an S3 URI."""
    return f"s3://{_s3_bucket()}/{key}"


def _raise_s3_error(exc: Exception, action: str, key: str | None = None):
    """Translate low-level S3 client exceptions into clearer project errors."""
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
            f"Falha ao {action} no S3:{target}. O bucket nao existe na conta atual ou nao esta na regiao "
            f"'{region}'."
        ) from exc

    if error_code in {"AccessDenied", "AllAccessDisabled"}:
        raise RuntimeError(
            f"Falha ao {action} no S3:{target}. A conta atual nao tem permissao para acessar "
            f"esse bucket."
        ) from exc

    if error_code == "NoSuchKey":
        raise FileNotFoundError(
            f"Falha ao {action} no S3:{target}. O objeto informado nao foi encontrado."
        ) from exc

    raise RuntimeError(
        f"Falha ao {action} no S3:{target}. Erro AWS: {error_code or type(exc).__name__}."
    ) from exc


def _put_object(*, key: str, body: bytes, content_type: str):
    """Write an object to the configured S3 bucket."""
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
    """List objects under a given S3 prefix."""
    try:
        response = _s3_client().list_objects_v2(Bucket=_s3_bucket(), Prefix=prefix)
    except Exception as exc:
        _raise_s3_error(exc, action="listar objetos", key=prefix)
    return response.get("Contents", [])


def _get_object_bytes(key: str) -> bytes:
    """Read raw bytes from an object in S3."""
    try:
        response = _s3_client().get_object(Bucket=_s3_bucket(), Key=key)
    except Exception as exc:
        _raise_s3_error(exc, action="ler objeto", key=key)
    return response["Body"].read()


def save_raw_data(data: dict):
    """Persist the raw payload into the bronze layer."""
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
    """Return the latest bronze object reference from local storage or S3."""
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
    """Read a raw bronze payload from local storage or S3."""
    if isinstance(file_ref, str) and file_ref.startswith("s3://"):
        file_ref = file_ref.removeprefix(f"s3://{_s3_bucket()}/")

    if isinstance(file_ref, Path):
        with file_ref.open(encoding="utf-8") as file:
            return json.load(file)

    if storage_backend() == "s3" or isinstance(file_ref, str):
        return json.loads(_get_object_bytes(file_ref).decode("utf-8"))

    with file_ref.open(encoding="utf-8") as file:
        return json.load(file)


def list_processed_files() -> list[Path | str]:
    """List available silver parquet files from the configured storage backend."""
    if storage_backend() == "s3":
        prefix = f"{_s3_prefix()}/{_s3_layer_prefix('silver')}/"
        contents = _list_objects(prefix)
        return [item["Key"] for item in sorted(contents, key=lambda item: item["Key"])]

    return sorted(LOCAL_PROCESSED_DIR.glob("*.parquet"))


def read_processed_data(file_ref) -> pd.DataFrame:
    """Read a silver parquet file from local storage or S3."""
    if isinstance(file_ref, str) and file_ref.startswith("s3://"):
        file_ref = file_ref.removeprefix(f"s3://{_s3_bucket()}/")

    if isinstance(file_ref, Path):
        return pd.read_parquet(file_ref)

    if storage_backend() == "s3" or isinstance(file_ref, str):
        return pd.read_parquet(io.BytesIO(_get_object_bytes(file_ref)))

    return pd.read_parquet(file_ref)


def save_processed_data(df: pd.DataFrame):
    """Persist the silver dataframe as parquet."""
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


def save_gold_data(df: pd.DataFrame):
    """Persist the gold dataframe as parquet."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    reference_date = None if df.empty else df["reference_date"].max()

    if storage_backend() == "s3":
        key = _build_s3_key("gold", timestamp, "parquet", reference_date=reference_date)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        _put_object(key=key, body=buffer.getvalue(), content_type="application/octet-stream")
        return _to_s3_uri(key)

    LOCAL_GOLD_DIR.mkdir(parents=True, exist_ok=True)
    output_file = LOCAL_GOLD_DIR / f"brics_rates_gold_{timestamp}.parquet"
    df.to_parquet(output_file, index=False)
    return output_file
