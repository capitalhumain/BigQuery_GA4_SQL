-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH user_lifetime_value AS (
  -- Extract the maximum lifetime value (LTV) revenue for each user
  SELECT 
    user_pseudo_id, 
    MAX(user_ltv.revenue) AS max_ltv_revenue
  FROM 
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE 
    _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
  GROUP BY 
    user_pseudo_id
  HAVING 
    max_ltv_revenue > 0
),

first_page_views AS (
  -- Extract the first page location for each user
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
    ) AS first_page_location
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

user_revenue_and_page_data AS (
  -- Combine revenue data with the first page location for each user
  SELECT 
    ltv.user_pseudo_id, 
    ltv.max_ltv_revenue, 
    fpv.first_page_location
  FROM 
    user_lifetime_value ltv
  JOIN 
    first_page_views fpv 
  ON 
    ltv.user_pseudo_id = fpv.user_pseudo_id
)

-- Aggregate results to calculate metrics by first page location
SELECT 
  first_page_location,
  COUNT(user_pseudo_id) AS user_count, -- Total number of users
  ROUND(AVG(max_ltv_revenue), 2) AS avg_ltv_revenue -- Average maximum LTV revenue
FROM 
  user_revenue_and_page_data
GROUP BY 
  first_page_location
ORDER BY 
  user_count DESC; -- Sort by user count for actionable insights
