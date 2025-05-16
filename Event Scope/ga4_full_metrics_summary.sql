-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-10-01';
DECLARE end_date STRING DEFAULT '2024-10-31';

WITH
  -- Session-Level Data: Calculate session length separately
  session_data AS (
    SELECT
      user_pseudo_id,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
      PARSE_DATE("%Y%m%d", event_date) AS date_formatted,
      MIN(event_timestamp) AS session_start_time,
      MAX(event_timestamp) AS session_end_time,
      (MAX(event_timestamp) - MIN(event_timestamp)) / 1000000 AS session_length_in_seconds
    -- Replace with your own project and dataset ID
    FROM `project.dataset.events_*`
    WHERE _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
    GROUP BY user_pseudo_id, session_id, date_formatted
  ),

  -- Aggregate session data to get session-level metrics for each day
  session_aggregated AS (
    SELECT
      date_formatted,
      COUNT(DISTINCT CONCAT(user_pseudo_id, session_id)) AS sessions,
      AVG(session_length_in_seconds) AS averageSessionDuration
    FROM session_data
    GROUP BY date_formatted
  ),

  -- Event-Level Metrics
  event_data AS (
    SELECT
      user_pseudo_id,
      PARSE_DATE("%Y%m%d", event_date) AS date_formatted,
      event_name,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS session_number,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') AS session_engaged,
      ecommerce.purchase_revenue
    -- Replace with your own project and dataset ID
    FROM `project.dataset.events_*`
    WHERE _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
  ),

  -- Aggregate Event Data Per Day
  event_aggregated AS (
    SELECT
      ed.date_formatted,
      COUNT(DISTINCT ed.user_pseudo_id) AS totalUsers,
      COUNTIF(ed.event_name = 'page_view') AS screenPageViews,
      COUNTIF(ed.event_name = 'purchase') AS Purchases,
      SUM(ed.purchase_revenue) AS purchaseRevenue,
      COUNT(DISTINCT CASE WHEN ed.session_number = 1 THEN ed.user_pseudo_id END) AS NewUsers,
      COUNT(DISTINCT CASE WHEN ed.session_engaged = '1' THEN CONCAT(ed.user_pseudo_id, ed.session_id) END) AS engagedSessions,
      ROUND(SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN ed.session_engaged = '1' THEN CONCAT(ed.user_pseudo_id, ed.session_id) END), COUNT(DISTINCT CONCAT(ed.user_pseudo_id, ed.session_id))) * 100, 2) AS engagementRate,
      COUNT(*) AS eventCount
    FROM event_data ed
    GROUP BY ed.date_formatted
  )

-- Combine Session and Event Data
SELECT
  ea.date_formatted,
  sa.sessions,
  sa.averageSessionDuration AS averageSessionDuration_seconds,
  ea.totalUsers,
  ea.NewUsers,
  ea.Purchases,
  IFNULL(ea.purchaseRevenue, 0) AS purchaseRevenue,
  ea.screenPageViews,
  ea.engagedSessions,
  ea.engagementRate AS engagementRate_percent,
  ea.eventCount
FROM event_aggregated ea
LEFT JOIN session_aggregated sa ON ea.date_formatted = sa.date_formatted
ORDER BY ea.date_formatted;
