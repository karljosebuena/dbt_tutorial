WITH SUCCESS_CTE AS (
  SELECT
    COUNT(*) AS row_count,
    MAX(host) as host,
    route,
    MAX(status_code) AS status_code,
    MAX(timestamp) AS last_transaction
  FROM `development-395907.logs.proceso_logs_dbt_model`
  WHERE LENGTH(TRIM(route)) > 0 AND PARSE_NUMERIC(status_code) >= 200 AND PARSE_NUMERIC(status_code) <= 204
  GROUP BY route
),
FAIL_CTE AS (
  SELECT
    COUNT(*) AS row_count,
    MAX(host) as host,
    route,
    MAX(status_code) AS status_code,
    MAX(timestamp) AS last_transaction
  FROM `development-395907.logs.proceso_logs_dbt_model`
  WHERE LENGTH(TRIM(route)) > 0 AND (PARSE_NUMERIC(status_code) < 200 OR PARSE_NUMERIC(status_code) > 204)
  GROUP BY route
)

SELECT
  INITCAP(COALESCE(SUCCESS_CTE.route, FAIL_CTE.route)) AS `OPERATION`,
  COALESCE(SUCCESS_CTE.host, FAIL_CTE.host) AS host,
  COALESCE(SUCCESS_CTE.route, FAIL_CTE.route) AS route,
  COALESCE(SUCCESS_CTE.row_count, 0) + COALESCE(FAIL_CTE.row_count, 0) AS `total_transactions`,
  COALESCE(SUCCESS_CTE.row_count, 0) AS `success_count`,
  COALESCE(FAIL_CTE.row_count, 0) AS `fail_count`,
  COALESCE(SUCCESS_CTE.last_transaction, FAIL_CTE.last_transaction) AS last_transaction,
  CONCAT("<a target='_blank' rel='noopener noreferrer' href='https://covertech-873987ba3.sentry.io/issues/?environment=dev&project=4505866118758400&query=is%3Aunresolved+url%3A%22", "*", COALESCE(SUCCESS_CTE.host, FAIL_CTE.host) || "/" || COALESCE(SUCCESS_CTE.route, FAIL_CTE.route), "*", "%22&referrer=issue-list&statsPeriod=30d>View Issues</a>") AS `view_issues`
FROM SUCCESS_CTE
FULL OUTER JOIN FAIL_CTE ON SUCCESS_CTE.route = FAIL_CTE.route