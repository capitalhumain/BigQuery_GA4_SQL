-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH session_counts AS (
  -- Calculate the total number of sessions by event date
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date, -- Convert event_date to DATE format
    COUNT(DISTINCT CONCAT(
      user_pseudo_id, 
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    )) AS total_sessions
  FROM 
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE
    _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
  GROUP BY 
    event_date
),

converted_session_counts AS (
  -- Calculate the number of converted sessions (purchase or add to cart) by event date
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date, -- Convert event_date to DATE format
    COUNT(DISTINCT CONCAT(
      user_pseudo_id, 
     (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'))) AS converted_sessions
  FROM 
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE
    event_name IN ('purchase') 
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
  GROUP BY 
    event_date
)

-- Calculate session conversion rate by date
SELECT
  sc.event_date,
  ROUND(
    SAFE_DIVIDE(AVG(cs.converted_sessions), AVG(sc.total_sessions)) * 100, 3
  ) AS session_conversion_rate -- Conversion rate as a percentage
FROM 
  session_counts sc
LEFT JOIN 
  converted_session_counts cs
ON 
  sc.event_date = cs.event_date
GROUP BY 
  sc.event_date
ORDER BY 
  sc.event_date ASC;
