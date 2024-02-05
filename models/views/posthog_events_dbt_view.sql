SELECT
  COUNT(*) AS row_count,
  host,
  current_url,
  CASE WHEN current_url LIKE '%proceso-portal.web.app%'
    THEN current_url
    ELSE CONCAT('https://', host, current_url)
  END AS full_url,
  MAX(dateCreated) as last_transaction
FROM
  `development-395907.dbt_demo_bigquery.posthog_events_dbt_model`
GROUP BY
  host,
  current_url
