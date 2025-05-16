-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH user_sessions AS (
    -- Extract user session data including session ID, session number, and event date
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date, -- Convert event date to DATE format
        user_pseudo_id,
        CAST(
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS INT64
        ) AS session_id,
        CAST(
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS INT64
        ) AS session_number
    FROM
        `project.dataset.events_*`, -- Replace with your own project and dataset ID
    WHERE 
        _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Date range filter
),

user_type_counts AS (
    -- Count the number of returning and new users grouped by event date
    SELECT 
        event_date,
        COUNT(DISTINCT IF(session_number > 1, user_pseudo_id, NULL)) AS returning_user_count,
        COUNT(DISTINCT IF(session_number = 1, user_pseudo_id, NULL)) AS new_user_count
    FROM 
        user_sessions
    GROUP BY 
        event_date
)

-- Output the user counts by event date
SELECT 
    event_date,
    returning_user_count AS returning_users,
    new_user_count AS new_users
FROM 
    user_type_counts
ORDER BY 
    event_date ASC;
