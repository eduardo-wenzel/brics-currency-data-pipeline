import json
import logging
import os
import time
from datetime import UTC, datetime
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
CURRENCIES = os.getenv("CURRENCIES", "")

if not BASE_URL:
    raise EnvironmentError("Variavel de ambiente API_URL nao configurada.")


def _parse_currencies(raw_value: str) -> list[str]:
    return [item.strip().upper() for item in raw_value.split(",") if item.strip()]


def _normalize_payload(data: dict) -> dict:
    rates = data.get("conversion_rates") or data.get("rates")
    if not isinstance(rates, dict) or not rates:
        raise ValueError("Resposta da API sem taxas de cambio validas.")

    base_currency = data.get("base_code") or data.get("base")
    if not base_currency:
        raise ValueError("Resposta da API sem moeda base.")

    raw_reference = data.get("time_last_update_utc") or data.get("date")
    if raw_reference:
        try:
            if len(raw_reference) == 10 and raw_reference[4] == "-":
                reference_dt = datetime.strptime(raw_reference, "%Y-%m-%d").replace(tzinfo=UTC)
            else:
                reference_dt = datetime.strptime(raw_reference, "%a, %d %b %Y %H:%M:%S %z")
        except ValueError as exc:
            raise ValueError(f"Formato de data inesperado na API: {raw_reference}") from exc
    else:
        reference_dt = datetime.now(UTC)

    selected = _parse_currencies(CURRENCIES)
    if selected:
        rates = {k: v for k, v in rates.items() if k in selected}
        if not rates:
            raise ValueError("Nenhuma moeda da lista CURRENCIES foi encontrada na resposta da API.")

    return {
        "result": "success",
        "base_code": str(base_currency).upper(),
        "time_last_update_utc": reference_dt.strftime("%a, %d %b %Y %H:%M:%S %z"),
        "conversion_rates": rates,
    }


def fetch_exchange_rates():
    start = time.time()

    response = requests.get(BASE_URL, timeout=15)
    response.raise_for_status()
    data = response.json()

    # Algumas APIs retornam 'result=error' em vez de HTTP status != 200.
    if data.get("result") == "error":
        raise Exception(f"Falha na API: {data.get('error-type', 'erro desconhecido')}")

    normalized = _normalize_payload(data)

    duration = time.time() - start
    logging.info(f"{len(normalized['conversion_rates'])} moedas recebidas da API.")
    logging.info(f"Tempo de resposta da API: {duration:.2f} segundos.")

    return normalized


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
        save_raw_data(data)
        logging.info("Pipeline executado com sucesso.")
    except Exception:
        logging.exception("Erro no pipeline")
        raise


if __name__ == "__main__":
    main()
