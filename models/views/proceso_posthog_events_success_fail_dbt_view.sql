WITH SUCCESS_CTE AS (
    SELECT
      COUNT(*) AS row_count,
      MAX(host) as host,
      base_url,
      MAX(original_url) AS original_url,
      MAX(full_url) AS full_url,
      MAX(status_code) AS status_code,
      MAX(date_created) AS last_transaction
    FROM
      `development-395907.dbt_demo_bigquery.proceso_posthog_events_dbt_model`
    WHERE LENGTH(TRIM(base_url)) > 0 AND PARSE_NUMERIC(status_code) >= 200 AND PARSE_NUMERIC(status_code) <= 204
    GROUP BY
      base_url
),
FAIL_CTE AS (
    SELECT
      COUNT(*) AS row_count,
      MAX(host) as host,
      base_url,
      MAX(original_url) AS original_url,
      MAX(full_url) AS full_url,
      MAX(status_code) AS status_code,
      MAX(date_created) AS last_transaction
    FROM
      `development-395907.dbt_demo_bigquery.proceso_posthog_events_dbt_model`
    WHERE LENGTH(TRIM(base_url)) > 0 AND (PARSE_NUMERIC(status_code) < 200 OR PARSE_NUMERIC(status_code) > 204)
    GROUP BY
      base_url
)

SELECT
  UPPER(SUBSTR(COALESCE(SUCCESS_CTE.base_url, FAIL_CTE.base_url), 2, 1)) || LOWER(SUBSTR(COALESCE(SUCCESS_CTE.base_url, FAIL_CTE.base_url), 3)) AS `operation`,
  COALESCE(SUCCESS_CTE.host, FAIL_CTE.host) AS host,
  COALESCE(SUCCESS_CTE.base_url, FAIL_CTE.base_url) AS base_url,
  COALESCE(SUCCESS_CTE.original_url, FAIL_CTE.original_url) AS original_url,
  COALESCE(SUCCESS_CTE.full_url, FAIL_CTE.full_url) AS full_url,
  COALESCE(SUCCESS_CTE.row_count, 0) + COALESCE(FAIL_CTE.row_count, 0) AS `total_transactions`,
  COALESCE(SUCCESS_CTE.row_count, 0) AS `success_count`,
  COALESCE(FAIL_CTE.row_count, 0) AS `fail_count`,
  COALESCE(SUCCESS_CTE.last_transaction, FAIL_CTE.last_transaction) AS last_transaction,
  CONCAT("<a href='https://covertech-873987ba3.sentry.io/issues/?environment=prod&project=4505866118758400&query=is%3Aunresolved+url%3A%22", "*", COALESCE(SUCCESS_CTE.host, FAIL_CTE.host) || COALESCE(SUCCESS_CTE.base_url, FAIL_CTE.base_url), "*", "%22&referrer=issue-list&statsPeriod=30d'>View Issues</a>") AS `view_issues`
FROM SUCCESS_CTE
FULL OUTER JOIN FAIL_CTE ON SUCCESS_CTE.base_url = FAIL_CTE.base_url
