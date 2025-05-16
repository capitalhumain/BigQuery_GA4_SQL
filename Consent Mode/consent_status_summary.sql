-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

-- Query for consent summary: Total events, users, and sessions by consent status
SELECT
  privacy_info.analytics_storage AS analytics_storage_status,
  privacy_info.ads_storage AS ads_storage_status,
  COUNT(*) AS total_events,
  COUNT(DISTINCT user_pseudo_id) AS total_users,
  COUNT(DISTINCT CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'))) AS total_sessions
FROM
-- Replace with your own project and dataset ID
  `project.dataset.events_*` 
WHERE
  _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
GROUP BY
  analytics_storage_status,
  ads_storage_status
ORDER BY
  total_events DESC; -- Optional: Order by total events for clearer insight
