-- Declare date range variables to filter data for the year 2024
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH purchase_events AS (
  -- Extract users who made a purchase and their session numbers
  SELECT
    user_pseudo_id,
    -- First purchase session number
    (SELECT value.int_value 
     FROM UNNEST(event_params) 
     WHERE key = 'ga_session_number') AS first_purchase_session_number,
    MIN(event_timestamp) AS first_purchase_timestamp
  FROM 
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE 
    event_name = 'purchase'
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
  GROUP BY 
    user_pseudo_id, first_purchase_session_number
),

initial_landing_pages AS (
  -- Capture the first landing page for each user in their first session
  SELECT
    user_pseudo_id,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        FIRST_VALUE(params.value.string_value) OVER (
          PARTITION BY user_pseudo_id 
          ORDER BY event_timestamp
        ),
        r'(\?.*)$', '' -- Remove query parameters
      ),
      r'#.*$', '' -- Remove fragments
    ) AS landing_page_url
  FROM 
    `project.dataset.events_*`, -- Replace with your own project and dataset ID
    UNNEST(event_params) AS params
  WHERE 
    event_name = 'page_view'
    AND params.key = 'page_location'
    AND (SELECT value.int_value 
         FROM UNNEST(event_params) 
         WHERE key = 'ga_session_number') = 1
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
),

purchase_analysis AS (
  -- Join landing pages with purchase data to associate them
  SELECT
    ilp.landing_page_url AS landing_page,
    pe.user_pseudo_id,
    pe.first_purchase_session_number
  FROM 
    initial_landing_pages ilp
  JOIN 
    purchase_events pe 
  ON 
    ilp.user_pseudo_id = pe.user_pseudo_id
)

-- Aggregate results to calculate metrics
SELECT
  landing_page,
  COUNT(DISTINCT user_pseudo_id) AS purchase_count, -- Total number of purchases per landing page
  ROUND(AVG(first_purchase_session_number), 2) AS avg_session_number -- Average session number for purchases
FROM 
  purchase_analysis
GROUP BY 
  landing_page
ORDER BY 
  purchase_count DESC; -- Sort by purchase count for better insights
