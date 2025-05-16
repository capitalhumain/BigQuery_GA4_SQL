-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH page_metrics AS (
  -- Calculate metrics related to page engagement and views
  SELECT
    -- Extract page location from event parameters
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    -- Calculate total engagement time in seconds
    SUM((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec')) / 1000 AS engagement_time_seconds,
    -- Count page views
    COUNTIF(event_name = 'page_view') AS page_views
  FROM
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE
    _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
  GROUP BY
    page_location
)

-- Aggregate engagement and page views metrics
SELECT
  page_location,
  SUM(engagement_time_seconds) AS total_engagement_time_seconds,
  SUM(page_views) AS total_page_views,
  SAFE_DIVIDE(SUM(engagement_time_seconds), SUM(page_views)) AS average_engagement_time_per_page
FROM
  page_metrics
GROUP BY
  page_location
ORDER BY
  total_engagement_time_seconds DESC; -- Optional: Order by total engagement time for insights
