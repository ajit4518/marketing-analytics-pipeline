from datetime import date

import pandas as pd
import pytest
from google.cloud import bigquery

from connectors.transforms import RAW_COLUMNS
from loaders import bigquery as loader


class FakeJob:
    def result(self):
        return None


class FakeClient:
    calls = []

    def load_table_from_dataframe(self, df, destination, job_config):
        FakeClient.calls.append((df, destination, job_config))
        return FakeJob()


@pytest.fixture(autouse=True)
def fake_client(monkeypatch):
    FakeClient.calls = []
    monkeypatch.setattr(loader.bigquery, "Client", FakeClient)


def _frame(day: str) -> pd.DataFrame:
    return pd.DataFrame(
        [[day, "1", "Brand", 100, 5, 2.5, 1.0, 20.0]], columns=RAW_COLUMNS
    )


def test_load_day_overwrites_only_that_partition():
    rows = loader.load_day(_frame("2026-06-01"), "p.marketing_raw.google_ads_campaigns", date(2026, 6, 1))

    df, destination, job_config = FakeClient.calls[0]
    assert rows == 1
    assert destination == "p.marketing_raw.google_ads_campaigns$20260601"
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert job_config.time_partitioning.field == "date"
    assert "_loaded_at" in df.columns


def test_load_day_rejects_rows_from_another_day():
    with pytest.raises(ValueError):
        loader.load_day(_frame("2026-06-02"), "p.d.t", date(2026, 6, 1))
    assert FakeClient.calls == []
