import json

from pipeline import extract, storage


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


def test_save_raw_data_writes_json(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_LAKE_BACKEND", "local")
    payload = {"base": "USD", "rates": {"BRL": 5.0}}

    path = extract.save_raw_data(payload)

    assert path.exists()
    assert path.parent.name == "raw"
    with path.open("r", encoding="utf-8") as f:
        saved = json.load(f)
    assert saved == payload


def test_save_raw_data_writes_to_s3(monkeypatch):
    client = DummyS3Client()
    monkeypatch.setenv("DATA_LAKE_BACKEND", "s3")
    monkeypatch.setenv("AWS_S3_BUCKET", "demo-bucket")
    monkeypatch.setenv("AWS_S3_PREFIX", "brics")
    monkeypatch.setattr(storage, "_s3_client", lambda: client)

    file_ref = extract.save_raw_data({"base": "USD", "rates": {"BRL": 5.0}})

    assert file_ref.startswith("s3://demo-bucket/brics/")
    saved_key = next(key for bucket, key in client.objects if bucket == "demo-bucket")
    saved_payload = json.loads(client.objects[("demo-bucket", saved_key)]["Body"].decode("utf-8"))
    assert saved_payload["base"] == "USD"


def test_fetch_exchange_rates_success(monkeypatch):
    class DummyResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"base": "USD", "rates": {"BRL": 5.0}}

    monkeypatch.setattr(extract, "BASE_URL", "https://example.com")
    monkeypatch.setattr(extract.requests, "get", lambda *args, **kwargs: DummyResponse())

    data = extract.fetch_exchange_rates()

    assert data["base"] == "USD"
    assert "BRL" in data["rates"]
