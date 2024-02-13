SELECT
  COUNT(*) AS row_count,
  host,
  current_url,
  CASE
    WHEN LOWER(current_url) LIKE '%development-395907.web.app%'
    THEN current_url -- Do not prepend 'https://' if already present
    ELSE CONCAT('https://', host, current_url) -- Prepend 'https://' if not present
  END AS full_url,
  MAX(date_created) as last_transaction
FROM
  `development-395907.dbt_demo_bigquery.proceso_posthog_events_dbt_model`
GROUP BY
  host,
  current_url