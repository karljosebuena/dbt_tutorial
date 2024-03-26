WITH EventData AS (
  SELECT
  timestamp,
  JSON_EXTRACT_SCALAR(json_payload, '$.level') as log_level,
  JSON_EXTRACT_SCALAR(json_payload, '$.msg') as log_message,
  JSON_EXTRACT_SCALAR(json_payload, '$.req.headers.host') as host,
  JSON_EXTRACT_SCALAR(json_payload, '$.req.headers.x-cloud-trace-context') as cloud_trace_context,
  JSON_EXTRACT_SCALAR(json_payload, '$.req.headers.x-posthog-session-id') as posthog_session_id,
  JSON_EXTRACT_SCALAR(json_payload, '$.res.statusCode') as status_code,
  JSON_EXTRACT_SCALAR(json_payload, '$.responseTime') as response_time,
  JSON_EXTRACT_SCALAR(json_payload, '$.req.url') as url,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(json_payload, '$.req.url'), r'^/([^/?]+)') as route
  FROM
    `development-395907.proceso_logs._AllLogs`
  WHERE
    JSON_EXTRACT_SCALAR(json_payload, '$.req.headers.host') = 'procesoportalv2-5aa2zjftgq-ts.a.run.app'
)
SELECT *
FROM EventData
ORDER BY route
