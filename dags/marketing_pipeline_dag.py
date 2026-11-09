"""
Daily marketing analytics pipeline.

For each logical date (the day being processed, i.e. yesterday at run time):
  1. pull Google Ads and Meta campaign performance in parallel,
  2. replace that day's partition in the BigQuery raw tables,
  3. run `dbt build` on the staging models and the mart (models + tests).

Runs at 06:00 UTC, after both platforms have mostly settled the previous
day. Reruns and backfills are safe: each load overwrites one partition.

Airflow Variables:
  gcp_project        GCP project holding the raw dataset
  dbt_project_dir    path to this repo's dbt/ folder on the workers
"""
from __future__ import annotations

from datetime import date, timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

RAW_DATASET = "marketing_raw"

# channel -> (module, class). Loads into marketing_raw.<channel>_campaigns.
CONNECTORS = {
    "google_ads": ("connectors.google_ads", "GoogleAdsConnector"),
    "facebook_ads": ("connectors.facebook_ads", "FacebookAdsConnector"),
}


def _pull_and_load(channel: str, ds: str, **_) -> int:
    # Imported here so the scheduler can parse the DAG without the ad SDKs.
    from importlib import import_module

    from loaders.bigquery import load_day

    run_date = date.fromisoformat(ds)
    module_name, class_name = CONNECTORS[channel]
    connector = getattr(import_module(module_name), class_name)()

    df = connector.get_campaign_performance(run_date, run_date)
    table_id = f"{Variable.get('gcp_project')}.{RAW_DATASET}.{channel}_campaigns"
    return load_day(df, table_id, run_date)


with DAG(
    dag_id="marketing_analytics_pipeline",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["marketing", "google-ads", "meta-ads", "bigquery", "dbt"],
) as dag:
    loads = [
        PythonOperator(
            task_id=f"load_{channel}",
            python_callable=_pull_and_load,
            op_kwargs={"channel": channel},
        )
        for channel in CONNECTORS
    ]

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd {{ var.value.dbt_project_dir }} && "
            "dbt build --select +mart_campaign_performance"
        ),
        env={"GCP_PROJECT": "{{ var.value.gcp_project }}"},
        append_env=True,
    )

    loads >> dbt_build
