"""
Idempotent loads into date-partitioned BigQuery raw tables.

Each load replaces exactly one day's partition (table$YYYYMMDD with
WRITE_TRUNCATE), so rerunning or backfilling a day never duplicates rows.
Money stays FLOAT64 here; the dbt staging models cast it to NUMERIC.
"""
from __future__ import annotations

from datetime import date

import pandas as pd
from google.cloud import bigquery

from connectors.transforms import RAW_COLUMNS

RAW_SCHEMA = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_name", "STRING"),
    bigquery.SchemaField("impressions", "INT64"),
    bigquery.SchemaField("clicks", "INT64"),
    bigquery.SchemaField("cost_usd", "FLOAT64"),
    bigquery.SchemaField("conversions", "FLOAT64"),
    bigquery.SchemaField("conversion_value", "FLOAT64"),
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
]


def load_day(df: pd.DataFrame, table_id: str, run_date: date) -> int:
    """Replace the `run_date` partition of `table_id` with `df`. Returns rows loaded."""
    df = df[RAW_COLUMNS].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    if (df["date"] != run_date).any():
        raise ValueError(f"rows outside {run_date} would be written to its partition")
    df["_loaded_at"] = pd.Timestamp.now(tz="UTC")

    job_config = bigquery.LoadJobConfig(
        schema=RAW_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(field="date"),
    )
    client = bigquery.Client()
    partition = f"{table_id}${run_date:%Y%m%d}"
    client.load_table_from_dataframe(df, partition, job_config=job_config).result()
    return len(df)
