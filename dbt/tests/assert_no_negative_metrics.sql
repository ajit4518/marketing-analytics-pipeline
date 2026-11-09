-- Spend, clicks and impressions can be zero but never negative.
select *
from {{ ref('mart_campaign_performance') }}
where spend_usd < 0 or clicks < 0 or impressions < 0
