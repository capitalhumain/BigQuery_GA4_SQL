-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

-- Query to count events from non-consented users and identify countries with the most events
SELECT
  PARSE_DATE('%Y%m%d', event_date) AS date_formatted,
  geo.country AS country,
  COUNT(*) AS total_events
FROM
-- Replace with your own project and dataset ID
  `project.dataset.events_*` 
WHERE privacy_info.analytics_storage = 'No'
  AND privacy_info.ads_storage = 'No'
  AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
GROUP BY date_formatted, country
ORDER BY
  total_events DESC; -- Optional: Order by total events for easier analysis
