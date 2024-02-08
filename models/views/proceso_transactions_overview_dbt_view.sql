WITH CTE AS (
  SELECT
    SUM(CAST(total_transactions AS INT64)) AS total_transactions_sum,
    SUM(CAST(success AS INT64)) AS success_sum,
    SUM(CAST(issues AS INT64)) AS issues_sum
  FROM
    `development-395907.dbt_demo_bigquery.proceso_joined_posthog_sentry_events_dbt_view`
)
SELECT
*
  -- CONCAT(CAST(ROUND(CAST((CTE.success_sum/CTE.total_transactions_sum) * 100 AS FLOAT64), 0) AS STRING), '%') as success_percentage,
  -- CONCAT(CAST(ROUND(CAST((CTE.issues_sum/CTE.total_transactions_sum) * 100 AS FLOAT64), 0) AS STRING), '%') as issues_percentage
FROM CTE