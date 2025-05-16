-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH raw_events AS (
  -- Extract raw event data with relevant parameters
  SELECT
    event_date,
    event_timestamp,
    user_pseudo_id,
    -- Unique session identifier combining user ID and session ID
    CONCAT(
      user_pseudo_id, '-', 
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ) AS session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS session_number,
    event_name,
    -- Normalized page location
    REGEXP_REPLACE(
      REGEXP_REPLACE(LOWER((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')), r'(\?.*)$', ''),
      r'/$', ''
    ) AS page_location,
    ecommerce.purchase_revenue_in_usd AS revenue_usd,
    geo.country,
    device.category AS device,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS utm_campaign,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS utm_medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS utm_source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer
  FROM 
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE
    _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
    AND user_pseudo_id IS NOT NULL
    AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1
),

session_data AS (
  -- Assign relevant session-level attributes
  SELECT
    event_date,
    event_timestamp,
    user_pseudo_id,
    session_id,
    session_number,
    event_name,
    page_location,
    revenue_usd,
    -- Assign the first and last values of relevant attributes within the session
    FIRST_VALUE(country) OVER(PARTITION BY session_id ORDER BY event_timestamp) AS country,
    FIRST_VALUE(device) OVER(PARTITION BY session_id ORDER BY event_timestamp) AS device,
    LAST_VALUE(utm_campaign) OVER(PARTITION BY session_id ORDER BY event_timestamp) AS utm_campaign,
    LAST_VALUE(utm_medium) OVER(PARTITION BY session_id ORDER BY event_timestamp) AS utm_medium,
    LAST_VALUE(utm_source) OVER(PARTITION BY session_id ORDER BY event_timestamp) AS utm_source,
    LAST_VALUE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(page_referrer), r'(\?.*)$', ''), r'/$', '')) 
      OVER(PARTITION BY session_id ORDER BY event_timestamp) AS page_referrer
  FROM 
    raw_events
),

navigation_data AS (
  -- Derive navigation patterns with previous page details
  SELECT
    event_date,
    event_timestamp,
    user_pseudo_id,
    session_id,
    session_number,
    event_name,
    page_location,
    -- Previous page in the session
    LAG(page_location) OVER(PARTITION BY session_id ORDER BY event_timestamp) AS previous_page_location,
    revenue_usd,
    country,
    device,
    utm_campaign,
    utm_medium,
    utm_source,
    page_referrer
  FROM 
    session_data
),

filtered_pages AS (
  -- Filter out duplicate consecutive page visits within the same session
  SELECT
    event_date,
    event_timestamp,
    user_pseudo_id,
    session_id,
    page_location,
    ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY event_timestamp) AS visit_order,
    country,
    device,
    utm_campaign,
    utm_medium,
    utm_source,
    page_referrer
  FROM 
    navigation_data
  WHERE 
    previous_page_location IS NULL OR previous_page_location <> page_location
),

page_groups AS (
  -- Group pages by session for step-wise navigation tracking
  SELECT
    event_date,
    event_timestamp,
    user_pseudo_id,
    session_id,
    page_location,
    visit_order,
    country,
    device,
    utm_campaign,
    utm_medium,
    utm_source,
    page_referrer
  FROM 
    filtered_pages
),

session_page_steps AS (
  -- Map each session to specific navigation steps
  SELECT
    user_pseudo_id,
    session_id,
    country,
    device,
    utm_campaign,
    utm_medium,
    utm_source,
    page_referrer,
    MAX(CASE WHEN visit_order = 1 THEN page_location END) AS step1,
    MAX(CASE WHEN visit_order = 2 THEN page_location END) AS step2,
    MAX(CASE WHEN visit_order = 3 THEN page_location END) AS step3,
    MAX(CASE WHEN visit_order = 4 THEN page_location END) AS step4,
    MAX(CASE WHEN visit_order = 5 THEN page_location END) AS step5
  FROM 
    page_groups
  GROUP BY 
    user_pseudo_id, session_id, country, device, utm_campaign, utm_medium, utm_source, page_referrer
)

-- Analyze navigation frequency across steps
SELECT
  step1 AS stage1,
  step2 AS stage2,
  step3 AS stage3,
  step4 AS stage4,
  step5 AS stage5,
  COUNT(*) AS navigation_frequency 
FROM 
  session_page_steps
GROUP BY 
  stage1, stage2, stage3, stage4, stage5
HAVING 
  stage1 IS NOT NULL
ORDER BY 
  navigation_frequency DESC;
