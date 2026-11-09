# Marketing Analytics Pipeline

Daily Airflow pipeline that pulls Google Ads and Meta (Facebook) Ads campaign
performance into BigQuery and builds a unified campaign mart with dbt
(spend, revenue, CTR, CPC, CPM, CAC, ROAS and a 7-day ROAS).

## How it works

```
Google Ads API (GAQL)        Meta Marketing API (Insights)
        |                              |
connectors/google_ads.py     connectors/facebook_ads.py
        |                              |
        +------ loaders/bigquery.py ---+   replaces one day's partition
                       |
        marketing_raw.google_ads_campaigns
        marketing_raw.facebook_ads_campaigns
                       |
            dbt: stg_google_ads, stg_facebook_ads   (views)
                       |
            dbt: mart_campaign_performance          (table, partitioned by date)
```

The Airflow DAG (`dags/marketing_pipeline_dag.py`) runs at 06:00 UTC. For the
logical date it loads both channels in parallel, then runs
`dbt build --select +mart_campaign_performance`, which builds the models and
runs their tests.

Design choices:

- **Idempotent loads.** Each run overwrites the `table$YYYYMMDD` partition for
  its day, so reruns and backfills never duplicate rows.
- **One raw schema for both channels.** Both connectors return the same
  columns (`connectors/transforms.py`), so staging is a thin rename/cast and
  adding a channel is a new connector plus one staging model.
- **Meta purchases from `actions`.** Meta reports conversions as a list of
  action types. The connector counts the first purchase type present
  (`omni_purchase`, `purchase`, pixel purchase) so the same purchase is not
  counted twice.
- **Spend-weighted 7-day ROAS.** `sum(revenue) / sum(spend)` over a
  7-calendar-day `RANGE` window, rather than an average of daily ratios.

## Metrics

| Column | Definition |
|---|---|
| `ctr` | clicks / impressions |
| `cpc_usd` | spend / clicks |
| `cpm_usd` | spend * 1000 / impressions |
| `cac_usd` | spend / conversions |
| `roas` | revenue / spend |
| `roas_7d` | sum(revenue) / sum(spend) over the last 7 days, per channel and campaign |

Ratios use `safe_divide`, so they are null instead of erroring when the
denominator is zero.

## Data quality

- dbt tests: `not_null` on keys and spend, `accepted_values` on channel,
  one row per channel, campaign and day, and no negative spend, clicks or
  impressions.
- Source freshness on `_loaded_at`: warns after 30 hours, errors after 54.
- `loaders/bigquery.py` refuses to write rows dated outside the partition
  being replaced.
- Unit tests for the API-to-schema mapping: `pytest`.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest
```

Credentials (environment variables on the Airflow workers):

```bash
GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET,
GOOGLE_ADS_REFRESH_TOKEN, GOOGLE_ADS_CUSTOMER_ID, GOOGLE_ADS_LOGIN_CUSTOMER_ID (optional)
META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN, META_AD_ACCOUNT_ID
```

BigQuery uses Application Default Credentials. Airflow Variables:
`gcp_project` and `dbt_project_dir` (path to `dbt/`).

Run dbt on its own:

```bash
cd dbt
export GCP_PROJECT=my-project
dbt source freshness
dbt build
```

Load a single day by hand:

```python
from datetime import date
from connectors.google_ads import GoogleAdsConnector
from loaders.bigquery import load_day

day = date(2026, 6, 1)
df = GoogleAdsConnector().get_campaign_performance(day, day)
load_day(df, "my-project.marketing_raw.google_ads_campaigns", day)
```

## Adding a channel

1. Add `connectors/<channel>.py` returning `transforms.RAW_COLUMNS`.
2. Add a task for it in the DAG's channel list.
3. Add the raw table to `dbt/models/staging/_sources.yml`, a
   `stg_<channel>.sql` model, and a `union all` branch in the mart.
