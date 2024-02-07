SELECT
  COUNT(*) AS row_count,
  host,
  current_url,
  CONCAT('https://', host, current_url) as full_url,
  MAX(date_created) as last_transaction
FROM
  `development-395907.dbt_demo_bigquery.proceso_posthog_events_dbt_model`
GROUP BY
  host,
  current_url
