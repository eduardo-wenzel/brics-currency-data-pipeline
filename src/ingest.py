import requests
import json
from datetime import datetime
from pathlib import Path
import logging

# Garante que a pasta de logs exista
log_dir = Path("logs")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=log_dir / "pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_URL = "https://open.er-api.com/v6/latest/USD"
SYMBOLS = "BRL,RUB,INR,CNY,ZAR"


def fetch_exchange_rates():
    response = requests.get(BASE_URL, timeout=10)
    response.raise_for_status()
    data = response.json()

    if data.get("result") != "success":
        raise Exception("Falha na API")

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
        logging.error(f"Erro no pipeline: {e}")
        raise


if __name__ == "__main__":
    main()