"""
Pure functions that turn API rows into the raw table schema.

Kept free of the Google Ads and Meta SDKs so they can be unit tested
without credentials or network access.

Both connectors emit the same columns, which is what the BigQuery raw
tables and the dbt staging models expect:

  date, campaign_id, campaign_name, impressions, clicks,
  cost_usd, conversions, conversion_value
"""
from __future__ import annotations

from typing import Iterable, Mapping, Optional

RAW_COLUMNS = [
    "date",
    "campaign_id",
    "campaign_name",
    "impressions",
    "clicks",
    "cost_usd",
    "conversions",
    "conversion_value",
]

MICROS_PER_UNIT = 1_000_000

# Meta reports purchases under different action types depending on how the
# pixel / Conversions API is set up. Count the first one present so the same
# purchase is never counted twice.
META_PURCHASE_ACTION_TYPES = (
    "omni_purchase",
    "purchase",
    "offsite_conversion.fb_pixel_purchase",
)


def micros_to_units(micros: Optional[int]) -> float:
    """Google Ads money fields are in micros (1/1,000,000 of the currency unit)."""
    return (micros or 0) / MICROS_PER_UNIT


def google_campaign_record(
    *,
    date: str,
    campaign_id: int,
    campaign_name: str,
    impressions: int,
    clicks: int,
    cost_micros: int,
    conversions: float,
    conversions_value: float,
) -> dict:
    return {
        "date": date,
        "campaign_id": str(campaign_id),
        "campaign_name": campaign_name,
        "impressions": int(impressions or 0),
        "clicks": int(clicks or 0),
        "cost_usd": micros_to_units(cost_micros),
        "conversions": float(conversions or 0),
        "conversion_value": float(conversions_value or 0),
    }


def meta_action_total(
    actions: Optional[Iterable[Mapping]],
    action_types: Iterable[str] = META_PURCHASE_ACTION_TYPES,
) -> float:
    """
    Sum for the first matching action type in a Meta `actions` or
    `action_values` list, e.g. [{"action_type": "purchase", "value": "3"}].
    Returns 0.0 when none of the types is present.
    """
    by_type = {a.get("action_type"): a.get("value") for a in (actions or [])}
    for action_type in action_types:
        if action_type in by_type:
            return float(by_type[action_type] or 0)
    return 0.0


def meta_campaign_record(row: Mapping) -> dict:
    """Map one Meta Insights row (level=campaign, time_increment=1)."""
    return {
        "date": row["date_start"],
        "campaign_id": str(row["campaign_id"]),
        "campaign_name": row.get("campaign_name"),
        "impressions": int(row.get("impressions") or 0),
        "clicks": int(row.get("clicks") or 0),
        "cost_usd": float(row.get("spend") or 0),
        "conversions": meta_action_total(row.get("actions")),
        "conversion_value": meta_action_total(row.get("action_values")),
    }
