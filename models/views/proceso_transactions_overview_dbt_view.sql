WITH PROCESO_TRANSACTIONS_OVERVIEW_CTE AS (
  SELECT
    SUM(CAST(`Total Transactions` AS INT64)) AS total_transactions_sum,
    SUM(CAST(`Success` AS INT64)) AS success_sum,
    SUM(CAST(`Issues` AS INT64)) AS issues_sum
  FROM
    `development-395907.dbt_demo_bigquery.proceso_joined_posthog_sentry_events_dbt_view`
),
IndividualRows AS (
  SELECT 'Success' AS metric, success_sum AS value FROM PROCESO_TRANSACTIONS_OVERVIEW_CTE
  UNION ALL
  SELECT 'Issues' AS metric, issues_sum AS value FROM PROCESO_TRANSACTIONS_OVERVIEW_CTE
)
SELECT * FROM IndividualRows
