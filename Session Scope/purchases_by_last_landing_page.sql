-- Declare date range variables to filter data for the year 2024
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH purchase_sessions AS (
    -- Extract session IDs for purchase events
    SELECT 
        event_timestamp AS purchase_timestamp,
        CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id
    -- Replace with your own project and dataset ID
    FROM `project.dataset.events_*`
    WHERE event_name = 'purchase'
      AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
),

first_pageview_in_session AS (
    -- Identify landing pages of sessions that resulted in purchases
    SELECT
        CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
        CASE 
          WHEN (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') = 1 
          THEN (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') 
        END AS landing_page
    -- Replace with your own project and dataset ID
    FROM `project.dataset.events_*` e
    JOIN purchase_sessions ps
      ON CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id')) = ps.session_id
    WHERE e.event_name = 'page_view'
      AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
)

-- Aggregate purchases by landing page
SELECT
    fp.landing_page,
    COUNT(*) AS total_purchases
FROM purchase_sessions ps
JOIN first_pageview_in_session fp
  ON ps.session_id = fp.session_id
WHERE fp.landing_page IS NOT NULL
GROUP BY fp.landing_page
ORDER BY total_purchases DESC;
,,
