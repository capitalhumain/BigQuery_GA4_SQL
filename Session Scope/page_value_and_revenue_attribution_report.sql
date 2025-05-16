-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

-- Get purchase events with revenue values
WITH purchase_events AS (
  SELECT
    CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
    event_timestamp,
    SUM(event_value_in_usd) AS event_value
-- Replace with your own project and dataset ID
  FROM `project.dataset.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
    AND event_value_in_usd IS NOT NULL
    AND event_name = 'purchase'
  GROUP BY session_id, event_timestamp
),

-- Get page view events with page location
page_view_events AS (
  SELECT
    CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
    event_timestamp,
    REGEXP_REPLACE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'\?.*$', '') AS page_location
-- Replace with your own project and dataset ID
  FROM `project.dataset.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
    AND event_name = 'page_view'
),

-- Join page views with purchase events to create a pseudo session ID
session_page_revenue AS (
  SELECT
    CONCAT(pv.session_id, '-', pe.event_timestamp) AS pseudo_session_id,
    pe.event_value,
    pv.page_location,
    CASE WHEN pv.event_timestamp < pe.event_timestamp THEN TRUE ELSE FALSE END AS is_before_revenue_event
  FROM page_view_events pv
  FULL OUTER JOIN purchase_events pe ON pv.session_id = pe.session_id
  ORDER BY pv.session_id
),

-- Calculate revenue attribution for pages viewed before revenue events
attributed_page_revenue AS (
  SELECT
    pseudo_session_id,
    page_location,
    ROUND(event_value / COUNT(page_location) OVER (PARTITION BY pseudo_session_id), 2) AS page_revenue,
    COUNT(DISTINCT CASE WHEN is_before_revenue_event THEN pseudo_session_id END) OVER (PARTITION BY page_location) AS purchase_influence
  FROM session_page_revenue
  WHERE is_before_revenue_event
),

-- Aggregate revenue by page location
page_revenue_summary AS (
  SELECT
    page_location,
    SUM(page_revenue) AS total_page_revenue,
    MAX(purchase_influence) AS purchase_influence -- Max because count is repeated for each row in the CTE
  FROM attributed_page_revenue
  GROUP BY page_location
),

-- Get counts of page views by page location
page_view_counts AS (
  SELECT
    page_location,
    COUNT(page_location) AS views
  FROM page_view_events
  GROUP BY page_location
)
-- Final output: Join revenue summary and view counts to get page value
SELECT
  pvc.page_location,
  pvc.views,
  prs.purchase_influence,
  ROUND(prs.total_page_revenue, 2) AS page_revenue,
  ROUND(prs.total_page_revenue / pvc.views, 3) AS page_value
FROM page_view_counts pvc
LEFT JOIN page_revenue_summary prs ON pvc.page_location = prs.page_location
WHERE prs.total_page_revenue IS NOT NULL
ORDER BY views DESC;
