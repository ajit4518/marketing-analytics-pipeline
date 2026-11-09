"""
Google Ads API connector.

Pulls daily campaign performance with GAQL through GoogleAdsService
search_stream, and returns a DataFrame in the shared raw schema
(see connectors/transforms.py). Loading into BigQuery is done by
loaders/bigquery.py.

Required env vars:
  GOOGLE_ADS_DEVELOPER_TOKEN
  GOOGLE_ADS_CLIENT_ID
  GOOGLE_ADS_CLIENT_SECRET
  GOOGLE_ADS_REFRESH_TOKEN
  GOOGLE_ADS_CUSTOMER_ID          (10 digits, no dashes)
  GOOGLE_ADS_LOGIN_CUSTOMER_ID    (optional, manager account id)
"""
from __future__ import annotations

import os
from datetime import date

import pandas as pd
from google.ads.googleads.client import GoogleAdsClient

from connectors.transforms import RAW_COLUMNS, google_campaign_record

# All campaigns, not just ENABLED ones: a campaign paused today still spent
# money yesterday, and dropping it would understate spend.
CAMPAIGN_QUERY = """
    SELECT
        segments.date,
        campaign.id,
        campaign.name,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversions_value
    FROM campaign
    WHERE segments.date BETWEEN '{start}' AND '{end}'
"""


class GoogleAdsConnector:
    def __init__(self) -> None:
        config = {
            "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
            "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
            "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
            "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
            "use_proto_plus": True,
        }
        if os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID"):
            config["login_customer_id"] = os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"]
        self._client = GoogleAdsClient.load_from_dict(config)
        self._customer_id = os.environ["GOOGLE_ADS_CUSTOMER_ID"]

    def get_campaign_performance(self, start: date, end: date) -> pd.DataFrame:
        query = CAMPAIGN_QUERY.format(start=start.isoformat(), end=end.isoformat())
        service = self._client.get_service("GoogleAdsService")
        stream = service.search_stream(customer_id=self._customer_id, query=query)

        records = [
            google_campaign_record(
                date=row.segments.date,
                campaign_id=row.campaign.id,
                campaign_name=row.campaign.name,
                impressions=row.metrics.impressions,
                clicks=row.metrics.clicks,
                cost_micros=row.metrics.cost_micros,
                conversions=row.metrics.conversions,
                conversions_value=row.metrics.conversions_value,
            )
            for batch in stream
            for row in batch.results
        ]
        return pd.DataFrame.from_records(records, columns=RAW_COLUMNS)
