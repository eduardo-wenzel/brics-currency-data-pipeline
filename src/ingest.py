import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

# Garante que a pasta de logs exista
log_dir = Path("logs")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=log_dir / "pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_URL = os.getenv("API_URL")

if not BASE_URL:
    raise EnvironmentError("Variavel de ambiente API_URL nao configurada.")


def fetch_exchange_rates():
    start = time.time()

    response = requests.get(BASE_URL, timeout=15)
    response.raise_for_status()
    data = response.json()

    # Algumas APIs retornam 'result=error' em vez de HTTP status != 200.
    if data.get("result") == "error":
        raise Exception(f"Falha na API: {data.get('error-type', 'erro desconhecido')}")

    duration = time.time() - start
    logging.info(f"Payload bruto recebido da API em {duration:.2f} segundos.")

    return data


def save_raw_data(data: dict):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = Path("data/raw")
    raw_path.mkdir(parents=True, exist_ok=True)

    file_path = raw_path / f"brics_rates_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    logging.info(f"Arquivo raw salvo em: {file_path}")
    return file_path


def main():
    try:
        data = fetch_exchange_rates()
        save_raw_data(data)
        logging.info("Ingest executado com sucesso.")
    except Exception:
        logging.exception("Erro no ingest")
        raise


if __name__ == "__main__":
    main()
