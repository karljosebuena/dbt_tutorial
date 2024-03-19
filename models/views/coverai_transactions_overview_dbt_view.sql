WITH COVERAI_TRANSACTIONS_OVERVIEW_CTE AS (
  SELECT
    SUM(total_transactions) AS total_transactions_sum,
    SUM(success_count) AS success_sum,
    SUM(fail_count) AS issues_sum
  FROM
    `development-395907.dbt_demo_bigquery.coverai_posthog_events_success_fail_dbt_view`
),
IndividualRows AS (
  SELECT 'Success' AS metric, success_sum AS value FROM COVERAI_TRANSACTIONS_OVERVIEW_CTE
  UNION ALL
  SELECT 'Issues' AS metric, issues_sum AS value FROM COVERAI_TRANSACTIONS_OVERVIEW_CTE
)
SELECT * FROM IndividualRows
