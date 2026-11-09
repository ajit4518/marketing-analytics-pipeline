-- Fails if a load duplicated a campaign-day.
select channel, campaign_id, date, count(*) as n
from {{ ref('mart_campaign_performance') }}
group by 1, 2, 3
having count(*) > 1
