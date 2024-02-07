SELECT
  COUNT(*) AS row_count,
  tags_pivoted.url,
  tags_pivoted.transaction,
  CONCAT(tags_pivoted.url, tags_pivoted.transaction) as full_url,
  MAX(date_created) as last_transaction
FROM `development-395907.dbt_demo_bigquery.proceso_sentry_events_dbt_model`
WHERE tags_pivoted.url = "https://app.proceso.com.au/"
GROUP BY
  tags_pivoted.url,
  tags_pivoted.transaction