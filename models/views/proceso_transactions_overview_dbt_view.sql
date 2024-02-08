WITH CTE AS (
  SELECT
    SUM(CAST(total_transactions AS INT64)) AS total_transactions_sum,
    SUM(CAST(success AS INT64)) AS success_sum,
    SUM(CAST(issues AS INT64)) AS issues_sum
  FROM
    `development-395907.dbt_demo_bigquery.proceso_joined_posthog_sentry_events_dbt_view`
),
IndividualRows AS (
  SELECT 'Total Transactions' AS metric, total_transactions_sum AS value FROM CTE
  UNION ALL
  SELECT 'Success' AS metric, success_sum AS value FROM CTE
  UNION ALL
  SELECT 'Issues' AS metric, issues_sum AS value FROM CTE
)
SELECT * FROM IndividualRows;
