import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from pipeline import storage as storage_module

get_latest_raw_file_from_storage = storage_module.get_latest_raw_file
read_raw_data = storage_module.read_raw_data
persist_processed_data = storage_module.save_processed_data

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

CURRENCIES = os.getenv("CURRENCIES", "")


def get_latest_raw_file():
    """Return the latest raw payload reference from the configured bronze storage."""
    latest_file = get_latest_raw_file_from_storage()
    logging.info(f"Arquivo raw mais recente: {latest_file}")
    return latest_file


def _parse_currencies(raw_value: str) -> list[str]:
    """Parse the configured currency list into normalized currency codes."""
    return [item.strip().upper() for item in raw_value.split(",") if item.strip()]


def _parse_reference_date(raw_value: str):
    """Parse supported source date formats into a date object."""
    try:
        return datetime.strptime(raw_value, "%a, %d %b %Y %H:%M:%S %z").date()
    except ValueError:
        return datetime.strptime(raw_value, "%Y-%m-%d").date()


def _normalize_raw_payload(data: dict) -> dict:
    """Normalize API payload variants into a single transform contract."""
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


def transform_raw_file(file_path):
    """Transform a raw JSON payload into the normalized silver dataframe."""
    raw_data = read_raw_data(file_path)

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


def transform_latest_file():
    """Transform the most recent raw file available in bronze storage."""
    file_path = get_latest_raw_file()
    return transform_raw_file(file_path)


def save_processed_data(df):
    """Persist the silver dataframe in parquet format."""
    output_file = persist_processed_data(df)
    logging.info(f"Arquivo parquet salvo em: {output_file}")
    return output_file


def main():
    """Run the transform step as a module entrypoint."""
    df = transform_latest_file()
    save_processed_data(df)


if __name__ == "__main__":
    main()
