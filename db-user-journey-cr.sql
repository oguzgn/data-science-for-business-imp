WITH user_funnel AS (
  SELECT
    user_id,
    operating_system,
    country,
    MIN(CASE WHEN event_name = 'free_trial' THEN event_time END) as trial_start_time,
    MIN(CASE WHEN event_name = 'subscribe' THEN event_time END) as subscribe_time,
    MIN(CASE WHEN event_name = 'renewal' THEN event_time END) as renewal_time,
    MIN(CASE WHEN event_name = 'auto_renew_off' THEN event_time END) as cancel_time,
    MIN(CASE WHEN event_name = 'free_trial' THEN product_identifier END) as trial_product,
    MIN(CASE WHEN event_name = 'subscribe' THEN product_identifier END) as sub_product
  FROM `data-sciene-for-business-imp.app_analytics.dashboard_design`
  GROUP BY user_id, operating_system, country
),

user_journey_analysis AS (
  SELECT
    operating_system,
    country,
    COUNT(DISTINCT user_id) as total_users,
    
    COUNT(DISTINCT CASE 
      WHEN trial_start_time IS NOT NULL 
      THEN user_id 
    END) as total_trial,

    COUNT(DISTINCT CASE 
      WHEN subscribe_time IS NOT NULL 
      THEN user_id 
    END) as total_subs,
    
    COUNT(DISTINCT CASE 
      WHEN trial_start_time IS NOT NULL AND subscribe_time > trial_start_time 
      THEN user_id 
    END) as trial_to_paid,
    
    COUNT(DISTINCT CASE 
      WHEN trial_start_time IS NOT NULL 
      AND cancel_time IS NOT NULL 
      AND (subscribe_time IS NULL OR cancel_time < subscribe_time)
      THEN user_id 
    END) as trial_to_cancel,
    
    COUNT(DISTINCT CASE 
      WHEN subscribe_time IS NOT NULL 
      AND cancel_time > subscribe_time 
      THEN user_id 
    END) as sub_to_cancel,
    
    COUNT(DISTINCT CASE 
      WHEN renewal_time IS NOT NULL 
      THEN user_id 
    END) as renewed_users
  FROM user_funnel
  GROUP BY operating_system, country
)

SELECT
  operating_system,
  country,
  total_users,
  total_trial,
  total_subs,
  trial_to_paid,
  trial_to_cancel,
  sub_to_cancel,
  renewed_users,
  
  ROUND(trial_to_paid / NULLIF(total_trial, 0), 2) as trial_to_paid_rate,
  ROUND(trial_to_cancel / NULLIF(total_trial, 0), 2) as trial_cancel_rate,
  ROUND(sub_to_cancel / NULLIF(trial_to_paid, 0), 2) as paid_cancel_rate,
  ROUND(renewed_users / NULLIF(trial_to_paid, 0), 2) as renewal_rate

FROM user_journey_analysis
ORDER BY total_users DESC;
