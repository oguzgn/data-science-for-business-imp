WITH user_payments AS (
  SELECT 
    user_id,
    experiment_variant,
    DATE(TIMESTAMP_MILLIS(CAST(event_time AS INT64))) AS event_date,
    (SELECT value.string_value 
     FROM UNNEST(properties) 
     WHERE key = 'productDuration') AS product_duration,
    (SELECT CAST(value.float_value AS FLOAT64)
     FROM UNNEST(properties) 
     WHERE key = 'revenue') AS revenue,
    ROW_NUMBER() OVER(
      PARTITION BY user_id 
      ORDER BY TIMESTAMP_MILLIS(CAST(event_time AS INT64))
    ) AS payment_order
  FROM `data-sciene-for-business-imp.app_analytics.dataset_experiment`
  WHERE event_name = 'subscribe'
),

revenue_metrics AS (
  SELECT 
    experiment_variant,
    product_duration,
    AVG(revenue) AS arppu,
    SUM(revenue) AS total_revenue,
    COUNT(DISTINCT user_id) AS paying_users
  FROM user_payments
  WHERE payment_order = 1  -- for first subs
  GROUP BY experiment_variant, product_duration
),

total_users AS (
  SELECT 
    experiment_variant,
    COUNT(DISTINCT user_id) AS total_users
  FROM `data-sciene-for-business-imp.app_analytics.dataset_experiment`
  GROUP BY experiment_variant
)

SELECT 
  r.experiment_variant,
  r.product_duration,
  ROUND(r.arppu, 2) AS arppu,
  ROUND(r.total_revenue, 2) AS total_revenue,
  r.paying_users,
  t.total_users,
  ROUND(r.total_revenue / NULLIF(t.total_users, 0), 2) AS arpu,
  ROUND(CAST(r.paying_users AS FLOAT64) / NULLIF(t.total_users, 0) * 100, 2) AS conversion_rate
FROM revenue_metrics r
LEFT JOIN total_users t ON r.experiment_variant = t.experiment_variant
WHERE r.product_duration IS NOT NULL
ORDER BY 
  r.experiment_variant,
  r.product_duration;
