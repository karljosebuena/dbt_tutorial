-- SELECT
--   COUNT(*) AS row_count,
--   host,
--   current_url,
--   CASE
--     WHEN LOWER(current_url) LIKE '%development-395907.web.app%'
--     THEN current_url
--     ELSE CONCAT('https://', host, current_url)
--   END AS full_url,
--   MAX(date_created) as last_transaction
-- FROM
--   `development-395907.dbt_demo_bigquery.proceso_posthog_events_dbt_model`
-- GROUP BY
--   host,
--   current_url

-- BELOW SCRIPT IS FOR TESTING PURPOSES ONLY
-- TO MAP DATA FROM SENTRY TO POSTHOG, SO WE HAVE VARIATIONS OF DATA IN PRESET CHARTS

WITH CTE AS (
  SELECT
  *,
  CASE
    WHEN REGEXP_CONTAINS(current_url, '#') THEN
      REGEXP_EXTRACT(current_url, r'^(.*?)#')
    ELSE
      current_url
  END AS current_url_extracted
FROM
  `development-395907.dbt_demo_bigquery.proceso_posthog_events_dbt_model`
)

SELECT
  COUNT(*) AS row_count,
  host,
  current_url_extracted,
  CASE
    WHEN LOWER(current_url_extracted) LIKE '%development-395907.web.app%'
    THEN current_url_extracted
    ELSE CONCAT('https://', host, current_url_extracted)
  END AS full_url,
  MAX(date_created) as last_transaction
FROM
  CTE
GROUP BY
  host,
  current_url_extracted