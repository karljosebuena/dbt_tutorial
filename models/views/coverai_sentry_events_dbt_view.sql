SELECT
  COUNT(*) AS row_count,
  tags_pivoted.url,
  tags_pivoted.transaction,
  CONCAT(tags_pivoted.url, tags_pivoted.transaction) as full_url,
  MAX(date_created) as last_transaction
FROM `development-395907.dbt_demo_bigquery.coverai_sentry_events_dbt_model`
WHERE tags_pivoted.url = "https://coverai-eqg2th5m5a-ts.a.run.app/"
GROUP BY
  tags_pivoted.url,
  tags_pivoted.transaction