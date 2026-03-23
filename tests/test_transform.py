import json
from datetime import date
from io import BytesIO
from pathlib import Path
from uuid import uuid4

import pandas as pd

from pipeline import storage, transform


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

    def list_objects_v2(self, Bucket, Prefix):
        contents = []
        for index, ((bucket, key), _value) in enumerate(self.objects.items()):
            if bucket == Bucket and key.startswith(Prefix):
                contents.append({"Key": key, "LastModified": index})
        return {"Contents": contents}

    def get_object(self, Bucket, Key):
        body = self.objects[(Bucket, Key)]["Body"]
        return {"Body": DummyBody(body)}


def test_normalize_raw_payload_filters_currencies(monkeypatch):
    monkeypatch.setattr(transform, "CURRENCIES", "BRL,CNY")
    payload = {
        "base": "usd",
        "date": "2026-03-06",
        "rates": {"BRL": 5.0, "CNY": 7.2, "EUR": 0.9},
    }

    normalized = transform._normalize_raw_payload(payload)

    assert normalized["base_currency"] == "USD"
    assert set(normalized["rates"].keys()) == {"BRL", "CNY"}


def test_normalize_raw_payload_supports_exchange_rate_api_shape(monkeypatch):
    monkeypatch.setattr(transform, "CURRENCIES", "")
    payload = {
        "base_code": "usd",
        "time_last_update_utc": "Fri, 06 Mar 2026 00:00:01 +0000",
        "conversion_rates": {"BRL": 5.0, "INR": 87.1},
    }

    normalized = transform._normalize_raw_payload(payload)

    assert normalized == {
        "base_currency": "USD",
        "reference_date": date(2026, 3, 6),
        "rates": {"BRL": 5.0, "INR": 87.1},
    }


def test_normalize_raw_payload_raises_for_missing_rates(monkeypatch):
    monkeypatch.setattr(transform, "CURRENCIES", "")

    try:
        transform._normalize_raw_payload({"base": "USD", "date": "2026-03-06"})
    except ValueError as exc:
        assert "taxas de cambio" in str(exc)
    else:
        raise AssertionError("Era esperado ValueError para payload sem rates.")


def test_transform_latest_file_returns_dataframe(monkeypatch):
    test_root = Path("data/test_tmp") / f"transform-latest-{uuid4().hex}"
    raw_dir = test_root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_file = raw_dir / "sample.json"
    raw_file.write_text(
        json.dumps(
            {
                "base": "USD",
                "date": "2026-03-06",
                "rates": {"BRL": 5.0, "CNY": 7.2},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(transform, "CURRENCIES", "")
    monkeypatch.setenv("DATA_LAKE_BACKEND", "local")
    monkeypatch.setattr(transform, "get_latest_raw_file", lambda: raw_file)

    df = transform.transform_latest_file()

    assert len(df) == 2
    assert set(df.columns) == {"base_currency", "target_currency", "rate", "reference_date"}
    assert set(df["target_currency"]) == {"BRL", "CNY"}


def test_transform_raw_file_returns_dataframe(monkeypatch):
    test_root = Path("data/test_tmp") / f"transform-raw-{uuid4().hex}"
    test_root.mkdir(parents=True, exist_ok=True)
    raw_file = test_root / "sample.json"
    raw_file.write_text(
        json.dumps(
            {
                "base": "USD",
                "date": "2026-03-06",
                "rates": {"BRL": 5.0, "INR": 87.1},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(transform, "CURRENCIES", "")
    monkeypatch.setenv("DATA_LAKE_BACKEND", "local")

    df = transform.transform_raw_file(raw_file)

    assert len(df) == 2
    assert set(df["target_currency"]) == {"BRL", "INR"}


def test_transform_raw_file_uses_normalized_payload(monkeypatch):
    monkeypatch.setattr(transform, "read_raw_data", lambda _path: {"ignored": True})
    monkeypatch.setattr(
        transform,
        "_normalize_raw_payload",
        lambda _payload: {
            "base_currency": "USD",
            "reference_date": date(2026, 3, 6),
            "rates": {"BRL": 5.0, "CNY": 7.2},
        },
    )

    df = transform.transform_raw_file("raw.json")

    assert df.to_dict("records") == [
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


def test_save_processed_data_writes_to_s3(monkeypatch):
    client = DummyS3Client()
    dataframe = pd.DataFrame(
        [
            {
                "base_currency": "USD",
                "target_currency": "BRL",
                "rate": 5.0,
                "reference_date": "2026-03-06",
            }
        ]
    )

    monkeypatch.setenv("DATA_LAKE_BACKEND", "s3")
    monkeypatch.setenv("AWS_S3_BUCKET", "demo-bucket")
    monkeypatch.setenv("AWS_S3_PREFIX", "brics")
    monkeypatch.setattr(storage, "_s3_client", lambda: client)

    file_ref = transform.save_processed_data(dataframe)

    assert file_ref.startswith("s3://demo-bucket/brics/")
    saved_key = next(key for bucket, key in client.objects if bucket == "demo-bucket")
    restored = pd.read_parquet(BytesIO(client.objects[("demo-bucket", saved_key)]["Body"]))
    assert restored.iloc[0]["target_currency"] == "BRL"
