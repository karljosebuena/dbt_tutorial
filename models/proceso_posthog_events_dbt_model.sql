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
    distinct_id,
    JSON_EXTRACT_SCALAR(properties, '$.req.protocol') AS protocol,
    JSON_EXTRACT_SCALAR(properties, '$.req.host') AS host,
    JSON_EXTRACT_SCALAR(properties, '$.req.baseUrl') AS base_url,
    JSON_EXTRACT_SCALAR(properties, '$.req.path') AS path,
    JSON_EXTRACT_SCALAR(properties, '$.req.originalUrl') AS original_url,
    JSON_EXTRACT_SCALAR(properties, '$.req.fullUrl') AS full_url,
    JSON_EXTRACT_SCALAR(properties, '$.res.statusCode') AS status_code,
    JSON_EXTRACT_SCALAR(properties, '$.res.data') AS data,
    JSON_EXTRACT_SCALAR(person, '$.properties.name') AS user_name,
    JSON_EXTRACT_SCALAR(person, '$.properties.email') AS user_email,
    FORMAT_TIMESTAMP('%Y-%m-%d %I:%M:%S %p', timestamp) as date_created
  FROM
    `development-395907.posthog_airbyte_bigquery_sync.events`
  WHERE
    TIMESTAMP_TRUNC(timestamp, DAY) >= TIMESTAMP("2024-01-01")
    AND event = 'Proceso Backend'
    AND JSON_EXTRACT_SCALAR(properties, '$.req.host') = 'api.proceso.com.au'
)
SELECT *
FROM EventData
ORDER BY date_created
