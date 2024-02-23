{{
    config(
        materialized='table'
    )
}}

WITH EventData AS (
  SELECT
    _airbyte_raw_id,
    id,
    event,
    JSON_EXTRACT_SCALAR(properties, "$['$session_id']") AS session_id,
    distinct_id,
    JSON_EXTRACT_SCALAR(properties, "$['$host']") AS host,
    JSON_EXTRACT_SCALAR(properties, "$['$current_url']") AS current_url,
    JSON_EXTRACT_SCALAR(properties, "$['$initial_current_url']") AS initial_current_url,
    JSON_EXTRACT_SCALAR(person, "$.properties.name") AS user_name,
    JSON_EXTRACT_SCALAR(person, "$.properties.email") AS user_email,
    FORMAT_TIMESTAMP('%Y-%m-%d %I:%M:%S %p', timestamp) as date_created
  FROM
    `development-395907.posthog_airbyte_bigquery_sync.events`
  WHERE
    TIMESTAMP_TRUNC(timestamp, DAY) >= TIMESTAMP("2024-02-01")
    -- AND event = '$pageview' -- remove this filter
    AND JSON_EXTRACT_SCALAR(properties, "$['$host']") = "development-395907.web.app"
)
SELECT *
FROM EventData
ORDER BY date_created, session_id
