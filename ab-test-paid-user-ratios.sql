WITH all_users AS (
  SELECT
    DISTINCT user_id,
    experiment_variant
  FROM
    `data-sciene-for-business-imp.app_analytics.dataset_experiment`
),

subscribers AS (
  SELECT
    DISTINCT user_id,
    experiment_variant
  FROM
    `data-sciene-for-business-imp.app_analytics.dataset_experiment`
  WHERE
    event_name = 'subscribe'
),

refunds AS (
  SELECT
    DISTINCT user_id,
    experiment_variant
  FROM
    `data-sciene-for-business-imp.app_analytics.dataset_experiment`
  WHERE
    event_name = 'refund'
)

SELECT
  au.experiment_variant,
  COUNT(DISTINCT au.user_id) AS total_users,
  COUNT(DISTINCT s.user_id) AS total_paying_users, 
  COUNT(DISTINCT r.user_id) AS total_refund_users, 
  COUNT(DISTINCT au.user_id) - COUNT(DISTINCT s.user_id) AS non_paying_users, 
  ROUND(COUNT(DISTINCT s.user_id) / COUNT(DISTINCT au.user_id) * 100, 2) AS paying_user_ratio,
  ROUND(COUNT(DISTINCT r.user_id) / COUNT(DISTINCT au.user_id) * 100, 2) AS refund_user_ratio, 
  ROUND((COUNT(DISTINCT au.user_id) - COUNT(DISTINCT s.user_id)) / COUNT(DISTINCT au.user_id) * 100, 2) AS non_paying_user_ratio 
FROM
  all_users au
LEFT JOIN
  subscribers s
ON
  au.user_id = s.user_id AND au.experiment_variant = s.experiment_variant
LEFT JOIN
  refunds r
ON
  au.user_id = r.user_id AND au.experiment_variant = r.experiment_variant
GROUP BY
  au.experiment_variant;
