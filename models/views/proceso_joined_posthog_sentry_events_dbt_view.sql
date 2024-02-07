SELECT
  pV.full_url,
  pV.current_url as operation,
  pV.row_count as total_transactions,
  pV.row_count - sV.row_count as success,
  sV.row_count as issues,
  pV.last_transaction
 FROM `development-395907.dbt_demo_bigquery.proceso_posthog_events_dbt_view` pV
  INNER JOIN `development-395907.dbt_demo_bigquery.proceso_sentry_events_dbt_view` sV
    ON pV.full_url = sV.full_url