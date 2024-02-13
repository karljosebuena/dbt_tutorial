{{
    config(
        materialized='table'
    )
}}

WITH EventData AS (
  SELECT
    _airbyte_raw_id,
  FROM
    `development-395907.sentry_airbyte_bigquery_sync.events`,
    UNNEST(JSON_EXTRACT_ARRAY(tags, '$')) AS tag
  WHERE
    TIMESTAMP_TRUNC(dateCreated, DAY) >= TIMESTAMP("2024-02-01")
    AND type = 'error'
    AND JSON_EXTRACT_SCALAR(tag, '$.key') = "environment"
    AND JSON_EXTRACT_SCALAR(tag, '$.value') = "development"
)
SELECT
  sE._airbyte_raw_id,
  sE.id,
  sE.eventID,
  sE.event_type,
  sE.tags,
  sE.title,
  FORMAT_TIMESTAMP('%Y-%m-%d %I:%M:%S %p', sE.dateCreated) as date_created,
  (SELECT AS STRUCT
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'browser', JSON_EXTRACT_SCALAR(tag, '$.value') , NULL)) AS browser,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'browser.name', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS browser_name,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'device', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS device,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'device.family', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS device_family,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'environment', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS environment,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'handled', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS handled,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'level', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS level,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'mechanism', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS mechanism,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'os', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS os,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'os.name', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS os_name,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'posthog_distinct_id', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS posthog_distinct_id,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'posthog_session_id', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS posthog_session_id,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'release', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS release,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'transaction', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS transaction,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'url', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS url,
    MAX(IF(JSON_EXTRACT_SCALAR(tag, '$.key') = 'user', JSON_EXTRACT_SCALAR(tag, '$.value'), NULL)) AS user
  FROM  UNNEST(JSON_EXTRACT_ARRAY(sE.tags, '$')) AS tag
  ) AS tags_pivoted
  FROM `development-395907.sentry_airbyte_bigquery_sync.events` sE
    INNER JOIN EventData dE ON dE._airbyte_raw_id = sE._airbyte_raw_id
ORDER BY sE.dateCreated