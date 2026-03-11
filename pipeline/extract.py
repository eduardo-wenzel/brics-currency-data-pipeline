import logging
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

try:
    from pipeline.storage import save_raw_data as persist_raw_data
except ImportError:
    from storage import save_raw_data as persist_raw_data

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

log_dir = Path("logs")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=log_dir / "pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

BASE_URL = os.getenv("API_URL")


def fetch_exchange_rates():
    if not BASE_URL:
        raise OSError("Variavel de ambiente API_URL nao configurada.")

    start = time.time()

    response = requests.get(BASE_URL, timeout=15)
    response.raise_for_status()
    data = response.json()

    if data.get("result") == "error":
        raise Exception(f"Falha na API: {data.get('error-type', 'erro desconhecido')}")

    duration = time.time() - start
    logging.info(f"Payload bruto recebido da API em {duration:.2f} segundos.")

    return data


def save_raw_data(data: dict):
    file_path = persist_raw_data(data)
    logging.info(f"Arquivo raw salvo em: {file_path}")
    return file_path


def main():
    data = fetch_exchange_rates()
    save_raw_data(data)
    logging.info("Ingest executado com sucesso.")


if __name__ == "__main__":
    main()
