SELECT
  pV.full_url AS `URL`,
  UPPER(SUBSTR(pV.current_url, 2, 1)) || LOWER(SUBSTR(pV.current_url, 3)) AS `Operation`,
  pV.row_count AS `Total Transactions`,
  pV.row_count - sV.row_count AS `Success`,
  sV.row_count AS `Issues`,
  pV.last_transaction AS `Last Transaction`,
  CONCAT("<a href='https://covertech-873987ba3.sentry.io/issues/?environment=development&project=4505878431268864&query=is%3Aunresolved+url%3A%22", pV.full_url, "%22&referrer=issue-list&statsPeriod=30d'>View Issues</a>") AS `View Issues`
FROM
  `development-395907.dbt_demo_bigquery.proceso_posthog_events_dbt_view` pV
INNER JOIN
  `development-395907.dbt_demo_bigquery.proceso_sentry_events_dbt_view` sV
ON
  pV.full_url = sV.full_url