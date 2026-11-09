-- One row per channel, campaign and day, with unit economics.
-- Ratios are null when the denominator is zero rather than failing.

{{ config(
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['channel', 'campaign_id']
) }}

with daily as (
    select * from {{ ref('stg_google_ads') }}
    union all
    select * from {{ ref('stg_facebook_ads') }}
)

select
    date,
    channel,
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    conversions,
    spend_usd,
    revenue_usd,
    safe_divide(clicks, impressions)                as ctr,
    safe_divide(spend_usd, clicks)                  as cpc_usd,
    safe_divide(spend_usd * 1000, impressions)      as cpm_usd,
    safe_divide(spend_usd, conversions)             as cac_usd,
    safe_divide(revenue_usd, spend_usd)             as roas,
    -- 7-day ROAS weights each day by its spend (sum of revenue over sum of
    -- spend), and RANGE over calendar days keeps gaps from stretching the window.
    safe_divide(
        sum(revenue_usd) over seven_days,
        sum(spend_usd) over seven_days
    )                                               as roas_7d
from daily
window seven_days as (
    partition by channel, campaign_id
    order by unix_date(date)
    range between 6 preceding and current row
)
