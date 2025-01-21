WITH user_dates AS (
  SELECT 
    user_id,
    DATE(TIMESTAMP_MILLIS(CAST(first_event_time AS INT64))) as first_date,
    DATE(TIMESTAMP_MILLIS(CAST(event_time AS INT64))) as event_date,
    event_name,
    experiment_variant,
    (SELECT value.string_value 
     FROM UNNEST(properties) 
     WHERE key = 'productDuration') as product_duration
  FROM `data-sciene-for-business-imp.app_analytics.dataset_experiment`
  WHERE experiment_variant = 'B'  
),

paid_users AS (
  SELECT 
    user_id,
    first_date,
    MIN(event_date) as first_subscribe_date
  FROM user_dates
  WHERE event_name = 'subscribe' 
  AND product_duration = '12 Month'
  GROUP BY user_id, first_date
),

daily_new_users AS (
  SELECT 
    first_date,
    COUNT(DISTINCT user_id) as d0_users
  FROM paid_users
  GROUP BY first_date
),

user_churn AS (
  SELECT 
    pu.user_id,
    pu.first_date,
    MIN(CASE WHEN ud.event_name = 'auto_renew_off' THEN ud.event_date END) as churn_date
  FROM paid_users pu
  LEFT JOIN user_dates ud ON pu.user_id = ud.user_id
  WHERE ud.event_name = 'auto_renew_off'
  GROUP BY pu.user_id, pu.first_date
),

daily_active AS (
  SELECT 
    d.first_date,
    d.d0_users,
    d.d0_users - COUNT(CASE WHEN DATE_DIFF(uc.churn_date, d.first_date, DAY) <= 7 THEN 1 END) as d7_users,
    d.d0_users - COUNT(CASE WHEN DATE_DIFF(uc.churn_date, d.first_date, DAY) <= 14 THEN 1 END) as d14_users,
    d.d0_users - COUNT(CASE WHEN DATE_DIFF(uc.churn_date, d.first_date, DAY) <= 21 THEN 1 END) as d21_users,
    d.d0_users - COUNT(CASE WHEN DATE_DIFF(uc.churn_date, d.first_date, DAY) <= 28 THEN 1 END) as d28_users,
    d.d0_users - COUNT(CASE WHEN DATE_DIFF(uc.churn_date, d.first_date, DAY) <= 35 THEN 1 END) as d35_users,
    d.d0_users - COUNT(CASE WHEN DATE_DIFF(uc.churn_date, d.first_date, DAY) <= 42 THEN 1 END) as d42_users,
    d.d0_users - COUNT(CASE WHEN DATE_DIFF(uc.churn_date, d.first_date, DAY) <= 49 THEN 1 END) as d49_users,
    d.d0_users - COUNT(CASE WHEN DATE_DIFF(uc.churn_date, d.first_date, DAY) <= 56 THEN 1 END) as d56_users,
    d.d0_users - COUNT(CASE WHEN DATE_DIFF(uc.churn_date, d.first_date, DAY) <= 63 THEN 1 END) as d63_users,
    d.d0_users - COUNT(CASE WHEN DATE_DIFF(uc.churn_date, d.first_date, DAY) <= 70 THEN 1 END) as d70_users
  FROM daily_new_users d
  LEFT JOIN user_churn uc ON d.first_date = uc.first_date
  GROUP BY d.first_date, d.d0_users
)

SELECT 
  first_date,
  d0_users,
  d7_users,
  d14_users,
  d21_users,
  d28_users,
  d35_users,
  d42_users,
  d49_users,
  d56_users,
  d63_users,
  d70_users
FROM daily_active
WHERE d0_users > 0
ORDER BY first_date;
