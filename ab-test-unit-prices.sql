SELECT
  DISTINCT (
  SELECT
    value.float_value
  FROM
    UNNEST(properties)
  WHERE
    KEY = 'revenue') price,
  experiment_variant,
  CASE
    WHEN prop.key = 'productDuration' THEN prop.value.string_value
END
  AS product_duration,
FROM
  `data-sciene-for-business-imp.app_analytics.dataset_experiment`,
  UNNEST(properties) AS prop,
  UNNEST(properties) AS rev
WHERE
  prop.key = 'productDuration'
  AND rev.key = 'revenue'
  and  event_name = 'subscribe' 
  order by 2,3
