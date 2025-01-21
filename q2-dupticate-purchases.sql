SELECT 
    user_id,
    event_date,
    event_time,
    operating_system
  FROM `data-sciene-for-business-imp.app_analytics.data_reliability`
  WHERE DATE(event_date) = '2024-07-09'
    AND event_name = 'purchase'
    AND operating_system = 'ios'
ORDER BY  event_time;


