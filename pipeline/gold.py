import logging
from datetime import UTC, datetime

import pandas as pd

from pipeline import storage as storage_module

list_processed_files = storage_module.list_processed_files
read_processed_data = storage_module.read_processed_data
persist_gold_data = storage_module.save_gold_data


def _normalize_silver_schema(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Normalize legacy and current silver schemas into a single contract."""
    normalized = dataframe.copy()

    legacy_mapping = {
        "base": "base_currency",
        "currency": "target_currency",
        "value": "rate",
        "date": "reference_date",
    }
    rename_map = {
        source: target
        for source, target in legacy_mapping.items()
        if source in normalized.columns and target not in normalized.columns
    }
    if rename_map:
        normalized = normalized.rename(columns=rename_map)

    required_columns = ["base_currency", "target_currency", "rate", "reference_date"]
    missing = [column for column in required_columns if column not in normalized.columns]
    if missing:
        raise ValueError(
            "Arquivo silver com schema inesperado. Colunas ausentes: " + ", ".join(sorted(missing))
        )

    return normalized[required_columns]


def _load_silver_history() -> pd.DataFrame:
    """Load and consolidate silver history used to build the gold layer."""
    frames = []
    for file_ref in list_processed_files():
        dataframe = _normalize_silver_schema(read_processed_data(file_ref))
        if not dataframe.empty:
            frames.append(dataframe)

    if not frames:
        raise FileNotFoundError("Nenhum arquivo silver encontrado para gerar a camada gold.")

    history = pd.concat(frames, ignore_index=True)
    history["reference_date"] = pd.to_datetime(
        history["reference_date"], errors="coerce", utc=True
    ).dt.date
    history["rate"] = pd.to_numeric(history["rate"], errors="coerce")
    history = history.dropna(subset=["base_currency", "target_currency", "reference_date", "rate"])
    history = history.sort_values(["base_currency", "target_currency", "reference_date"])
    history = history.drop_duplicates(
        subset=["base_currency", "target_currency", "reference_date"], keep="last"
    )
    return history.reset_index(drop=True)


def build_gold_dataframe() -> pd.DataFrame:
    """Build the gold dataframe with trend and ranking metrics."""
    history = _load_silver_history()

    history["previous_rate"] = history.groupby(["base_currency", "target_currency"])["rate"].shift(
        1
    )
    history["rate_change"] = history["rate"] - history["previous_rate"]
    history["rate_change_pct"] = (history["rate_change"] / history["previous_rate"]) * 100

    history["trend_direction"] = "NEW"
    history.loc[history["rate_change"] > 0, "trend_direction"] = "UP"
    history.loc[history["rate_change"] < 0, "trend_direction"] = "DOWN"
    history.loc[
        history["previous_rate"].notna() & history["rate_change"].eq(0), "trend_direction"
    ] = "STABLE"

    history["rate_rank"] = (
        history.groupby(["base_currency", "reference_date"])["rate"]
        .rank(method="dense", ascending=False)
        .astype("int64")
    )
    history["currencies_in_snapshot"] = history.groupby(["base_currency", "reference_date"])[
        "target_currency"
    ].transform("count")
    history["snapshot_generated_at"] = datetime.now(UTC)

    gold = history[
        [
            "base_currency",
            "target_currency",
            "reference_date",
            "rate",
            "previous_rate",
            "rate_change",
            "rate_change_pct",
            "trend_direction",
            "rate_rank",
            "currencies_in_snapshot",
            "snapshot_generated_at",
        ]
    ].copy()

    logging.info(f"Gold gerou {len(gold)} registros analiticos.")
    return gold


def save_gold_data(df: pd.DataFrame):
    """Persist the gold dataframe in the configured storage backend."""
    output_file = persist_gold_data(df)
    logging.info(f"Arquivo gold salvo em: {output_file}")
    return output_file


def build_and_save_gold_data():
    """Generate and persist the gold layer in one step."""
    gold_df = build_gold_dataframe()
    output_file = save_gold_data(gold_df)
    return gold_df, output_file


def main():
    """Run the gold step as a module entrypoint."""
    build_and_save_gold_data()


if __name__ == "__main__":
    main()
