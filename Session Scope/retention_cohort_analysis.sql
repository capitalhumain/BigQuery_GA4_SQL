-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH session_data AS (
  -- Extract session-related data and calculate engagement metrics
  SELECT
    -- Unique session identifier combining user ID and session ID
    CONCAT(user_pseudo_id, '-', 
           (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
    user_pseudo_id,
    -- Session number
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS session_number,
    -- Maximum engagement time in milliseconds
    MAX((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec')) AS engagement_time_msec,
    -- Session date parsed into DATE format
    PARSE_DATE('%Y%m%d', event_date) AS session_date,
    -- First session date per user
    FIRST_VALUE(PARSE_DATE('%Y%m%d', event_date)) 
      OVER (PARTITION BY user_pseudo_id ORDER BY event_date) AS first_session_date
  FROM
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE
    _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
  GROUP BY
    user_pseudo_id, session_id, session_number, event_date
  ORDER BY
    user_pseudo_id, session_id, session_number
)

-- Calculate week-by-week engagement metrics
SELECT
  -- Year-week of the first session
  CONCAT(EXTRACT(ISOYEAR FROM first_session_date), '-', FORMAT('%02d', EXTRACT(ISOWEEK FROM first_session_date))) AS year_week,
  -- Weekly engagement metrics
  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 0 
         AND session_number >= 1 
         AND engagement_time_msec > 0 
    THEN user_pseudo_id END) AS week_0,
  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 1 
         AND session_number > 1 
         AND engagement_time_msec > 0 
    THEN user_pseudo_id END) AS week_1,
  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 2 
         AND session_number > 1 
         AND engagement_time_msec > 0 
    THEN user_pseudo_id END) AS week_2,
  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 3 
         AND session_number > 1 
         AND engagement_time_msec > 0 
    THEN user_pseudo_id END) AS week_3,
  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 4 
         AND session_number > 1 
         AND engagement_time_msec > 0 
    THEN user_pseudo_id END) AS week_4,
  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 5 
         AND session_number > 1 
         AND engagement_time_msec > 0 
    THEN user_pseudo_id END) AS week_5,
  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 6 
         AND session_number > 1 
         AND engagement_time_msec > 0 
    THEN user_pseudo_id END) AS week_6,
  COUNT(DISTINCT CASE 
    WHEN DATE_DIFF(session_date, first_session_date, ISOWEEK) = 7 
         AND session_number > 1 
         AND engagement_time_msec > 0 
    THEN user_pseudo_id END) AS week_7
FROM
  session_data
GROUP BY
  year_week
ORDER BY
  year_week;
