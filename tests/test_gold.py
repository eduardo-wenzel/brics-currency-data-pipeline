from datetime import date
from io import BytesIO

import pandas as pd

from pipeline import gold, storage


class DummyBody:
    def __init__(self, payload: bytes):
        self.payload = payload

    def read(self):
        return self.payload


class DummyS3Client:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[(Bucket, Key)] = {"Body": Body, "ContentType": ContentType}

    def get_object(self, Bucket, Key):
        body = self.objects[(Bucket, Key)]["Body"]
        return {"Body": DummyBody(body)}


def test_build_gold_dataframe_calculates_change_metrics(monkeypatch):
    history_by_file = {
        "silver-day-1.parquet": pd.DataFrame(
            [
                {
                    "base_currency": "USD",
                    "target_currency": "BRL",
                    "rate": 5.0,
                    "reference_date": date(2026, 3, 6),
                },
                {
                    "base_currency": "USD",
                    "target_currency": "CNY",
                    "rate": 7.2,
                    "reference_date": date(2026, 3, 6),
                },
            ]
        ),
        "silver-day-2.parquet": pd.DataFrame(
            [
                {
                    "base_currency": "USD",
                    "target_currency": "BRL",
                    "rate": 5.1,
                    "reference_date": date(2026, 3, 7),
                },
                {
                    "base_currency": "USD",
                    "target_currency": "CNY",
                    "rate": 7.0,
                    "reference_date": date(2026, 3, 7),
                },
            ]
        ),
    }

    monkeypatch.setattr(gold, "list_processed_files", lambda: list(history_by_file))
    monkeypatch.setattr(gold, "read_processed_data", lambda file_ref: history_by_file[file_ref])

    dataframe = gold.build_gold_dataframe()

    assert len(dataframe) == 4

    brl_day_2 = dataframe[
        (dataframe["target_currency"] == "BRL") & (dataframe["reference_date"] == date(2026, 3, 7))
    ].iloc[0]
    cny_day_2 = dataframe[
        (dataframe["target_currency"] == "CNY") & (dataframe["reference_date"] == date(2026, 3, 7))
    ].iloc[0]

    assert brl_day_2["previous_rate"] == 5.0
    assert round(brl_day_2["rate_change"], 2) == 0.1
    assert round(brl_day_2["rate_change_pct"], 2) == 2.0
    assert brl_day_2["trend_direction"] == "UP"
    assert brl_day_2["rate_rank"] == 2
    assert brl_day_2["currencies_in_snapshot"] == 2

    assert cny_day_2["previous_rate"] == 7.2
    assert cny_day_2["trend_direction"] == "DOWN"
    assert cny_day_2["rate_rank"] == 1


def test_build_gold_dataframe_supports_legacy_silver_schema(monkeypatch):
    legacy_df = pd.DataFrame(
        [
            {
                "base": "USD",
                "currency": "BRL",
                "value": 5.0,
                "date": "Fri, 06 Mar 2026 00:00:01 +0000",
            },
            {
                "base": "USD",
                "currency": "CNY",
                "value": 7.2,
                "date": "Fri, 06 Mar 2026 00:00:01 +0000",
            },
        ]
    )

    monkeypatch.setattr(gold, "list_processed_files", lambda: ["legacy.parquet"])
    monkeypatch.setattr(gold, "read_processed_data", lambda _file_ref: legacy_df)

    dataframe = gold.build_gold_dataframe()

    assert len(dataframe) == 2
    assert set(dataframe["target_currency"]) == {"BRL", "CNY"}
    assert set(dataframe["trend_direction"]) == {"NEW"}


def test_save_gold_data_writes_to_s3(monkeypatch):
    client = DummyS3Client()
    dataframe = pd.DataFrame(
        [
            {
                "base_currency": "USD",
                "target_currency": "BRL",
                "reference_date": date(2026, 3, 6),
                "rate": 5.0,
                "previous_rate": None,
                "rate_change": None,
                "rate_change_pct": None,
                "trend_direction": "NEW",
                "rate_rank": 1,
                "currencies_in_snapshot": 1,
                "snapshot_generated_at": pd.Timestamp("2026-03-06T00:00:00Z"),
            },
            {
                "base_currency": "USD",
                "target_currency": "CNY",
                "reference_date": date(2026, 3, 7),
                "rate": 7.2,
                "previous_rate": 7.0,
                "rate_change": 0.2,
                "rate_change_pct": 2.85,
                "trend_direction": "UP",
                "rate_rank": 1,
                "currencies_in_snapshot": 2,
                "snapshot_generated_at": pd.Timestamp("2026-03-07T00:00:00Z"),
            },
        ]
    )

    monkeypatch.setenv("DATA_LAKE_BACKEND", "s3")
    monkeypatch.setenv("AWS_S3_BUCKET", "demo-bucket")
    monkeypatch.setenv("AWS_S3_PREFIX", "brics")
    monkeypatch.setattr(storage, "_s3_client", lambda: client)

    file_ref = gold.save_gold_data(dataframe)

    assert file_ref.startswith("s3://demo-bucket/brics/")
    assert "year=2026/month=03/day=07" in file_ref
    saved_key = next(key for bucket, key in client.objects if bucket == "demo-bucket")
    restored = pd.read_parquet(BytesIO(client.objects[("demo-bucket", saved_key)]["Body"]))
    assert restored.iloc[0]["trend_direction"] == "NEW"
