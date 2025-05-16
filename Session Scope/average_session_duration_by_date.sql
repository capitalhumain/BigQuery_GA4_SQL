-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH session_durations AS (
  -- Calculate session durations in seconds
  SELECT
    -- Unique session identifier combining user ID and GA session ID
    CONCAT(
      user_pseudo_id,
      '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
    ) AS session_id,
    event_date,
    -- Calculate session length in seconds
    (MAX(event_timestamp) - MIN(event_timestamp)) / 1000000 AS session_length_in_seconds
  FROM
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE
    _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
  GROUP BY
    session_id, event_date
)

-- Aggregate session durations to calculate average session duration
SELECT
  -- Event date for analysis purposes
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  -- Calculate average session duration in seconds
  ROUND(SUM(session_length_in_seconds) / COUNT(DISTINCT session_id), 2) AS average_session_duration_seconds
FROM
  session_durations
GROUP BY
  event_date
ORDER BY
  event_date;
