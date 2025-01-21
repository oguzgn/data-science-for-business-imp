WITH DailyMetrics AS (
  SELECT 
    DATE(event_date) as date,
    COUNT(DISTINCT user_id) as daily_active_users,
    COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN user_id END) as total_unique_purchasers,
    COUNTIF(event_name = 'purchase') as total_purchase_count
  FROM `data-sciene-for-business-imp.app_analytics.data_reliability`
  GROUP BY DATE(event_date)
),
PurchaseByOS AS (
  SELECT
    DATE(event_date) as date,
    COUNT(DISTINCT CASE WHEN operating_system = 'ios' THEN user_id END) as ios_unique_purchasers,
    COUNT(DISTINCT CASE WHEN operating_system = 'android' THEN user_id END) as android_unique_purchasers,
    COUNTIF(operating_system = 'ios') as ios_purchase_count,
    COUNTIF(operating_system = 'android') as android_purchase_count
  FROM `data-sciene-for-business-imp.app_analytics.data_reliability`
  WHERE event_name = 'purchase'
  GROUP BY DATE(event_date)
)

SELECT 
  dm.date,
  dm.daily_active_users,
  dm.total_unique_purchasers,
  dm.total_purchase_count,
  ROUND(SAFE_DIVIDE(dm.total_purchase_count, dm.total_unique_purchasers), 2) as avg_purchase_per_purchaser,
  ROUND(SAFE_DIVIDE(dm.total_unique_purchasers, dm.daily_active_users) * 100, 2) as purchaser_rate,
  ROUND(SAFE_DIVIDE(dm.total_purchase_count, dm.daily_active_users) * 100, 2) as purchase_rate,
  COALESCE(pos.ios_unique_purchasers, 0) as ios_unique_purchasers,
  COALESCE(pos.ios_purchase_count, 0) as ios_purchase_count,
  COALESCE(pos.android_unique_purchasers, 0) as android_unique_purchasers,
  COALESCE(pos.android_purchase_count, 0) as android_purchase_count
FROM DailyMetrics dm
LEFT JOIN PurchaseByOS pos
  ON dm.date = pos.date
ORDER BY dm.date;
