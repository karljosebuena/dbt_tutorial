WITH PROCESO_TRANSACTIONS_OVERVIEW_CTE AS (
  SELECT
    SUM(CAST(`Total Transactions` AS INT64)) AS total_transactions_sum,
    SUM(CAST(`SUCCESS` AS INT64)) AS success_sum,
    SUM(CAST(`Issues` AS INT64)) AS issues_sum
  FROM
    `development-395907.dbt_demo_bigquery.proceso_joined_posthog_sentry_events_dbt_view`
)
SELECT
  success_sum as `SUCCESS`,
  issues_sum as `ISSUES`
FROM PROCESO_TRANSACTIONS_OVERVIEW_CTE
