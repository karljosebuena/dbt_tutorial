SELECT
  COUNT(*) AS row_count,
  tags_pivoted.url,
  tags_pivoted.transaction,
  CONCAT(tags_pivoted.url, tags_pivoted.transaction) as full_url,
  MAX(dateCreated) as last_transaction
FROM `development-395907.dbt_demo_bigquery.sentry_events_dbt_model`
GROUP BY
  tags_pivoted.url,
  tags_pivoted.transaction