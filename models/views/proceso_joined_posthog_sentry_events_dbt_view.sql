SELECT
  pV.full_url as `URL`,
  UPPER(SUBSTR(pV.current_url, 2, 1)) || LOWER(SUBSTR(pV.current_url, 3)) AS `Operation`,
  pV.row_count as `Total Transactions`,
  pV.row_count - sV.row_count as `Success`,
  sV.row_count as `Issues`,
  FORMAT_TIMESTAMP('%b %e, %Y %I:%M %p', pV.last_transaction) as `Last Transaction`
 FROM `development-395907.dbt_demo_bigquery.proceso_posthog_events_dbt_view` pV
  INNER JOIN `development-395907.dbt_demo_bigquery.proceso_sentry_events_dbt_view` sV
    ON pV.full_url = sV.full_url
