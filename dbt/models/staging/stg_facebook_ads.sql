select
    date,
    'facebook_ads' as channel,
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    cast(cost_usd as numeric) as spend_usd,
    conversions,
    cast(conversion_value as numeric) as revenue_usd
from {{ source('marketing_raw', 'facebook_ads_campaigns') }}
