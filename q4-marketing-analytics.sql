WITH first_touch AS (
  SELECT 
    user_id,
    FIRST_VALUE(
      CASE 
        WHEN tracker_name LIKE '%Apple Search%' THEN 'Apple Search'
        WHEN tracker_name LIKE '%Facebook%' THEN 'Facebook'
        WHEN tracker_name = 'Organic' THEN 'Organic'
      END
    ) OVER (PARTITION BY user_id ORDER BY logging_time ASC) as first_platform
  FROM `data-sciene-for-business-imp.app_analytics.marketing_users`,
  UNNEST(tracker_names) t
  WHERE tracker_name LIKE '%Apple Search%'
    OR tracker_name LIKE '%Facebook%'
    OR tracker_name = 'Organic'
),
user_platforms AS (
  SELECT DISTINCT user_id, first_platform as platform
  FROM first_touch
  WHERE first_platform IS NOT NULL
),
subscription_events AS (
  SELECT 
    e.user_id,
    COUNT(*) as subscription_count,
    SUM(prop.value.float_value) as total_revenue
  FROM `data-sciene-for-business-imp.app_analytics.marketing_events` e,
  UNNEST(properties) prop
  WHERE event_name = 'subscribe'
  AND prop.key = 'revenue'
  GROUP BY e.user_id
)
SELECT 
  p.platform,
  COUNT(DISTINCT p.user_id) as total_users,
  COUNT(DISTINCT CASE WHEN s.subscription_count > 0 THEN p.user_id END) as subscribers,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN s.subscription_count > 0 THEN p.user_id END) / COUNT(DISTINCT p.user_id), 2) as conversion_rate,
  SUM(s.subscription_count) as total_subscriptions,
  ROUND(SUM(COALESCE(s.total_revenue, 0)), 2) as total_revenue,
  ROUND(SUM(COALESCE(s.total_revenue, 0)) / COUNT(DISTINCT p.user_id), 2) as arpu,
  ROUND(SUM(COALESCE(s.total_revenue, 0)) / NULLIF(COUNT(DISTINCT CASE WHEN s.subscription_count > 0 THEN p.user_id END), 0), 2) as arppu,
  CASE 
    WHEN p.platform = 'Apple Search' THEN 3.1
    WHEN p.platform = 'Facebook' THEN 1.3
    ELSE 0
  END as cpi,
  ROUND(CASE 
    WHEN p.platform = 'Apple Search' THEN COUNT(DISTINCT p.user_id) * 3.1
    WHEN p.platform = 'Facebook' THEN COUNT(DISTINCT p.user_id) * 1.3
    ELSE 0
  END, 2) as total_cost,
  ROUND(CASE 
    WHEN p.platform IN ('Apple Search', 'Facebook') THEN 
      (SUM(COALESCE(s.total_revenue, 0)) - (
        CASE 
          WHEN p.platform = 'Apple Search' THEN COUNT(DISTINCT p.user_id) * 3.1
          WHEN p.platform = 'Facebook' THEN COUNT(DISTINCT p.user_id) * 1.3
          ELSE 0
        END
      )) / NULLIF((
        CASE 
          WHEN p.platform = 'Apple Search' THEN COUNT(DISTINCT p.user_id) * 3.1
          WHEN p.platform = 'Facebook' THEN COUNT(DISTINCT p.user_id) * 1.3
          ELSE 1
        END
      ), 0)
    ELSE NULL
  END, 2) as roi
FROM user_platforms p
LEFT JOIN subscription_events s ON p.user_id = s.user_id
GROUP BY p.platform
ORDER BY p.platform;
