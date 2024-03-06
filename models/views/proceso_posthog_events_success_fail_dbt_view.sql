WITH SUCCESS_CTE AS (
    SELECT
      COUNT(*) AS row_count,
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
  UPPER(SUBSTR(sc.base_url, 2, 1)) || LOWER(SUBSTR(sc.base_url, 3)) AS `operation`,
  sc.original_url,
  sc.full_url,
  COALESCE(sc.row_count, 0) + COALESCE(fc.row_count, 0) AS `total_transactions`,
  sc.row_count AS `success_count`,
  fc.row_count AS `fail_count`,
  sc.last_transaction AS `last_transaction`
FROM SUCCESS_CTE sc
  LEFT JOIN FAIL_CTE fc ON sc.base_url = fc.base_url