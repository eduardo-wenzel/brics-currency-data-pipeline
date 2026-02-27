import json
import pandas as pd
from pathlib import Path
from datetime import datetime

BRICS = ["BRL", "RUB", "INR", "CNY", "ZAR"]

def transform_latest_file():
    raw_path = Path("data/raw")
    latest_file = sorted(raw_path.glob("*.json"))[-1]

    with open(latest_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = []

    for currency in BRICS:
        value = data["rates"].get(currency)
        records.append({
            "date": data["time_last_update_utc"],
            "base": data["base_code"],
            "currency": currency,
            "value": value
        })

    df = pd.DataFrame(records)
    return df


def save_processed_data(df: pd.DataFrame):
    processed_path = Path("data/processed")
    processed_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = processed_path / f"brics_rates_{timestamp}.parquet"

    df.to_parquet(output_file, index=False)

    print(f"Arquivo salvo em: {output_file}")
    return output_file


if __name__ == "__main__":
    df = transform_latest_file()
    print(df)
    save_processed_data(df)