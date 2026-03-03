import requests
import json
import time
from pathlib import Path
import logging
import os
from dotenv import load_dotenv
from datetime import datetime

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
CURRENCIES = os.getenv("CURRENCIES")

if not BASE_URL or not CURRENCIES:
    raise EnvironmentError("Variáveis de ambiente não configuradas corretamente.")

def fetch_exchange_rates():
    start = time.time()

    response = requests.get(BASE_URL, timeout=10)
    response.raise_for_status()
    data = response.json()

    if data.get("result") != "success":
        raise Exception("Falha na API")

    duration = time.time() - start
    logging.info(f"{len(data.get('rates', {}))} moedas recebidas da API.")
    logging.info(f"Tempo de resposta da API: {duration:.2f} segundos.")

    return data

def save_raw_data(data: dict):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = Path("data/raw")
    raw_path.mkdir(parents=True, exist_ok=True)

    file_path = raw_path / f"brics_rates_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    logging.info(f"Arquivo salvo em: {file_path}")
    return file_path


def main():
    try:
        data = fetch_exchange_rates()
        file_path = save_raw_data(data)
        logging.info("Pipeline executado com sucesso.")
    except Exception as e:
       logging.exception("Erro no pipeline")
    raise


if __name__ == "__main__":
    main()