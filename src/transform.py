import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

CURRENCIES = os.getenv("CURRENCIES", "")


def get_latest_raw_file():
    raw_path = Path("data/raw")
    files = list(raw_path.glob("*.json"))

    if not files:
        raise FileNotFoundError("Nenhum arquivo raw encontrado.")

    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    logging.info(f"Arquivo raw mais recente: {latest_file}")
    return latest_file


def _parse_currencies(raw_value: str) -> list[str]:
    return [item.strip().upper() for item in raw_value.split(",") if item.strip()]


def _parse_reference_date(raw_value: str):
    try:
        return datetime.strptime(raw_value, "%a, %d %b %Y %H:%M:%S %z").date()
    except ValueError:
        return datetime.strptime(raw_value, "%Y-%m-%d").date()


def _normalize_raw_payload(data: dict) -> dict:
    rates = data.get("conversion_rates") or data.get("rates")
    if not isinstance(rates, dict) or not rates:
        raise ValueError("Estrutura inesperada: taxas de cambio nao encontradas.")

    base_currency = data.get("base_code") or data.get("base")
    if not base_currency:
        raise ValueError("Estrutura inesperada: moeda base nao encontrada.")

    raw_reference = data.get("time_last_update_utc") or data.get("date")
    if raw_reference:
        try:
            reference_date = _parse_reference_date(raw_reference)
        except ValueError as exc:
            raise ValueError(f"Formato de data inesperado: {raw_reference}") from exc
    else:
        reference_date = datetime.now(UTC).date()

    selected = _parse_currencies(CURRENCIES)
    if selected:
        rates = {k: v for k, v in rates.items() if k in selected}
        if not rates:
            raise ValueError("Nenhuma moeda da lista CURRENCIES foi encontrada no payload raw.")

    return {
        "base_currency": str(base_currency).upper(),
        "reference_date": reference_date,
        "rates": rates,
    }


def transform_latest_file():
    file_path = get_latest_raw_file()

    with open(file_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    normalized = _normalize_raw_payload(raw_data)

    records = [
        {
            "base_currency": normalized["base_currency"],
            "target_currency": currency,
            "rate": rate,
            "reference_date": normalized["reference_date"],
        }
        for currency, rate in normalized["rates"].items()
    ]

    df = pd.DataFrame(records)

    logging.info(f"Transform gerou {len(df)} registros.")
    logging.info(f"Colunas: {df.columns.tolist()}")

    return df


def save_processed_data(df):
    processed_path = Path("data/processed")
    processed_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = processed_path / f"brics_rates_{timestamp}.parquet"

    df.to_parquet(output_file, index=False)

    logging.info(f"Arquivo parquet salvo em: {output_file}")
    return output_file
