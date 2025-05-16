-- Declare date range variables to filter data for the year 2024
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH user_acquisition_metrics AS (
  -- Flattening event data for easier analysis
  SELECT
    event_name,
    user_pseudo_id,
    CONCAT(user_pseudo_id, '-', (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE KEY = 'ga_session_id')) AS session_id,
    CONCAT(COALESCE(traffic_source.source, '(direct)'), ' / ', COALESCE(traffic_source.medium, '(none)')) AS user_source,
    collected_traffic_source.gclid,
    ecommerce.purchase_revenue
  -- Replace with your own project and dataset ID
  FROM `project.dataset.events_*`
  WHERE _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
)

-- Aggregating metrics by user acquisition source
SELECT
  uam.user_source,
  COUNT(DISTINCT uam.user_pseudo_id) AS total_users,
  COUNT(DISTINCT uam.session_id) AS total_sessions,
  COUNTIF(uam.event_name = 'page_view') AS total_pageviews,
  COUNTIF(uam.event_name = 'signup') AS total_signups,
  COUNTIF(uam.event_name = 'purchase') AS total_purchases,
  ROUND(IFNULL(SUM(uam.purchase_revenue), 0), 2) AS total_revenue,
  ROUND(SAFE_DIVIDE(COUNTIF(uam.event_name = 'purchase'), COUNT(DISTINCT uam.user_pseudo_id)), 3) AS user_conversion_rate
FROM user_acquisition_metrics uam
GROUP BY uam.user_source
ORDER BY total_users DESC;
