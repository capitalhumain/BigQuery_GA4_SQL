-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH total_sessions AS (
  -- Calculate total sessions grouped by event date and page location
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    COUNT(DISTINCT CONCAT(
      user_pseudo_id, '-', 
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
    )) AS total_sessions
  FROM
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE
    _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
  GROUP BY
    event_date, page_location
),

session_engaged AS (
  -- Calculate engaged sessions grouped by event date and page location
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    COUNT(DISTINCT CONCAT(
      user_pseudo_id, '-', 
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
    )) AS session_engaged
  FROM
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE
    _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
    AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'session_engaged') = 1
  GROUP BY
    event_date, page_location
)

-- Calculate bounce rate by joining total sessions with engaged sessions
SELECT
  ts.event_date,
  ts.page_location,
  ts.total_sessions,
  se.session_engaged,
  ROUND(1 - SAFE_DIVIDE(se.session_engaged, ts.total_sessions), 2) AS bounce_rate
FROM
  total_sessions ts
LEFT JOIN
  session_engaged se
USING (event_date, page_location)
ORDER BY
  ts.event_date, ts.page_location;
