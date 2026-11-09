import pytest

from connectors.transforms import (
    RAW_COLUMNS,
    google_campaign_record,
    meta_action_total,
    meta_campaign_record,
)


def test_google_record_converts_micros_and_matches_schema():
    record = google_campaign_record(
        date="2026-06-01",
        campaign_id=123,
        campaign_name="Brand - Search",
        impressions=1000,
        clicks=50,
        cost_micros=12_340_000,
        conversions=2.0,
        conversions_value=99.5,
    )
    assert list(record) == RAW_COLUMNS
    assert record["cost_usd"] == pytest.approx(12.34)
    assert record["campaign_id"] == "123"


def test_meta_action_total_uses_first_purchase_type_only():
    actions = [
        {"action_type": "link_click", "value": "40"},
        {"action_type": "omni_purchase", "value": "3"},
        {"action_type": "purchase", "value": "3"},
    ]
    # omni_purchase and purchase describe the same purchases; never add both.
    assert meta_action_total(actions) == 3.0


def test_meta_action_total_handles_missing_actions():
    assert meta_action_total(None) == 0.0
    assert meta_action_total([{"action_type": "link_click", "value": "7"}]) == 0.0


def test_meta_record_maps_spend_and_purchases():
    row = {
        "date_start": "2026-06-01",
        "campaign_id": "2384",
        "campaign_name": "Prospecting",
        "impressions": "2000",
        "clicks": "30",
        "spend": "45.10",
        "actions": [{"action_type": "purchase", "value": "4"}],
        "action_values": [{"action_type": "purchase", "value": "210.00"}],
    }
    record = meta_campaign_record(row)
    assert list(record) == RAW_COLUMNS
    assert record["cost_usd"] == pytest.approx(45.10)
    assert record["conversions"] == 4.0
    assert record["conversion_value"] == pytest.approx(210.0)
    assert record["impressions"] == 2000
