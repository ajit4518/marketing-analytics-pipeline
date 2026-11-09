"""
Meta (Facebook) Marketing API connector.

Pulls daily campaign-level Insights with the facebook-business SDK and
returns a DataFrame in the shared raw schema (see connectors/transforms.py).
Conversions and conversion value come from the `actions` and
`action_values` lists, using the purchase action types.

Iterating the SDK cursor follows Meta's paging links, so every page is read.

Required env vars:
  META_APP_ID
  META_APP_SECRET
  META_ACCESS_TOKEN    (long-lived system user token)
  META_AD_ACCOUNT_ID   (act_XXXXXXXXXXXXXXXX)
"""
from __future__ import annotations

import os
from datetime import date

import pandas as pd
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi

from connectors.transforms import RAW_COLUMNS, meta_campaign_record

CAMPAIGN_FIELDS = [
    AdsInsights.Field.date_start,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.impressions,
    AdsInsights.Field.clicks,
    AdsInsights.Field.spend,
    AdsInsights.Field.actions,
    AdsInsights.Field.action_values,
]


class FacebookAdsConnector:
    def __init__(self) -> None:
        FacebookAdsApi.init(
            app_id=os.environ["META_APP_ID"],
            app_secret=os.environ["META_APP_SECRET"],
            access_token=os.environ["META_ACCESS_TOKEN"],
        )
        self._account = AdAccount(os.environ["META_AD_ACCOUNT_ID"])

    def get_campaign_performance(self, start: date, end: date) -> pd.DataFrame:
        params = {
            "level": "campaign",
            "time_range": {"since": start.isoformat(), "until": end.isoformat()},
            "time_increment": 1,
            "limit": 500,
        }
        cursor = self._account.get_insights(fields=CAMPAIGN_FIELDS, params=params)
        records = [meta_campaign_record(dict(row)) for row in cursor]
        return pd.DataFrame.from_records(records, columns=RAW_COLUMNS)
