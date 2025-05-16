-- Declare date range variables to filter data for the year 2024
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH signup_events AS (
  -- Extracting unique users who signed up
  SELECT DISTINCT user_pseudo_id
 -- Replace with your own project and dataset ID
  FROM `project.dataset.events_*`
  WHERE event_name = 'signup'
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
),

initial_landing_pages AS (
  -- Capturing the first page location for each user on their first session
  SELECT
    user_pseudo_id,
    FIRST_VALUE(params.value.string_value) OVER (
      PARTITION BY user_pseudo_id 
      ORDER BY event_timestamp
    ) AS landing_page_url
  FROM `project.dataset.events_*`,
  UNNEST(event_params) AS params
  WHERE event_name = 'page_view' 
    AND params.key = 'page_location'
    AND (SELECT value.int_value 
    -- Replace with your own project and dataset ID
         FROM UNNEST(event_params) 
         WHERE key = 'ga_session_number') = 1
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
)

-- Counting distinct users by their initial landing page that resulted in signups
SELECT
  ilp.landing_page_url AS landing_page,
  COUNT(DISTINCT ilp.user_pseudo_id) AS user_count
FROM initial_landing_pages ilp
JOIN signup_events se 
  ON ilp.user_pseudo_id = se.user_pseudo_id
GROUP BY landing_page
ORDER BY user_count DESC;
