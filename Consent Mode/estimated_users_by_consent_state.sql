-- This query estimates the number of users and events for consented and non-consented groups. 
-- The factor adjustment (3 for non-consented, 2 for consented) accounts for the assumption that 
-- non-consented users generate 1.5x more events due to repeated events like `session_start` and `first_visit`. 
-- Adjust the factors in the `ConsentFactors` CTE based on your specific assumptions or consent management platform data 
-- to better reflect user behavior on your site.



-- Declare date range variables 
DECLARE start_date STRING DEFAULT '2024-01-01';  -- Start of 2024
DECLARE end_date STRING DEFAULT '2024-12-31';    -- End of 2024

WITH ConsentFactors AS (
    -- Define the factor for event count estimation based on consent state
    SELECT 
        2.0 AS factor_value, 'Granted' AS consent_state -- Consented users
    UNION ALL
    SELECT 
        3.0 AS factor_value, 'Denied' AS consent_state -- Non-consented users
),

EventData AS (
    -- Extract relevant event data
    SELECT
        IF(e.user_pseudo_id IS NOT NULL, 'Granted', 'Denied') AS consent_state, -- Identify consent state
        COUNT(1) AS total_events, -- Total events
        COUNT(DISTINCT e.user_pseudo_id) AS distinct_users -- Distinct users
    FROM 
        `project.dataset.events_*` AS e  -- Replace with your project and dataset
    WHERE
        e.event_name = 'page_view'
        AND e.event_date BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')  -- Filter by date range (2024)
    GROUP BY 
        consent_state
),

ConsentAnalysis AS (
    -- Join event data with factors to calculate estimated users
    SELECT
        ed.consent_state,
        ed.total_events,
        ed.distinct_users,
        CAST(ROUND(ed.total_events / cf.factor_value) AS INT64) AS estimated_users -- Estimate user count
    FROM 
        EventData ed
    LEFT JOIN 
        ConsentFactors cf 
    ON 
        ed.consent_state = cf.consent_state
)

-- Final output with share calculation
SELECT
    consent_state,
    total_events,
    distinct_users,
    estimated_users,
    ROUND(SAFE_DIVIDE(total_events, SUM(total_events) OVER()), 4) AS event_share -- Share of events
FROM 
    ConsentAnalysis
ORDER BY 
    total_events DESC;  -- Order by total events, descending
