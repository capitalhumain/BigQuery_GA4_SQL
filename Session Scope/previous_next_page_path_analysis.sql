-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH page_view_data AS (
  -- Extract page view data with previous and next page navigation details
  SELECT
    -- Unique session identifier combining user ID and session ID
    CONCAT(
      user_pseudo_id, '-', 
        (SELECT value.int_value 
         FROM UNNEST(event_params) 
         WHERE key = 'ga_session_id') 
    ) AS session_id,
    user_pseudo_id,
    -- Extract and normalize the page location to get the page path
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        (SELECT value.string_value 
         FROM UNNEST(event_params) 
         WHERE key = 'page_location'),
        r'^https?://[^/]+', '' -- Remove the domain
      ),
      r'[\?].*', '' -- Remove query parameters
    ) AS page_path,
    event_timestamp
  FROM
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE
    event_name = 'page_view'
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
),

page_navigation AS (
  -- Derive navigation patterns using LAG and LEAD functions
  SELECT
    session_id,
    user_pseudo_id,
    -- Previous page
    LAG(page_path) OVER (
      PARTITION BY user_pseudo_id, session_id 
      ORDER BY event_timestamp ASC
    ) AS previous_page,
    -- Current page
    page_path AS current_page,
    -- Next page
    LEAD(page_path) OVER (
      PARTITION BY user_pseudo_id, session_id 
      ORDER BY event_timestamp ASC
    ) AS next_page,
    event_timestamp
  FROM 
    page_view_data
)

-- Aggregate navigation patterns and calculate session counts
SELECT 
  IFNULL(previous_page, '(entrance)') AS previous_page,
  current_page,
  IFNULL(next_page, '(exit)') AS next_page,
  COUNT(DISTINCT session_id) AS session_count
FROM 
  page_navigation
GROUP BY 
  previous_page,
  current_page,
  next_page
HAVING 
  current_page NOT IN (previous_page, next_page) -- Filter out loops
ORDER BY 
  session_count DESC; -- Order by the most common navigation patterns
