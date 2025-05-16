-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH device_category_user_metrics AS (
    -- Count distinct users by device category
    SELECT
        device.category AS device_category,
        COUNT(DISTINCT user_pseudo_id) AS user_count -- Number of users per device category
    FROM
        `project.dataset.events_*` -- Replace with your actual project and dataset ID
    WHERE
        _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Date range filter
    GROUP BY 
        device.category
)

-- Calculate the share of each device category in total users
SELECT
    device_category,
    user_count,
    ROUND(SAFE_DIVIDE(user_count, SUM(user_count) OVER()), 4) AS category_share -- Share of each device category
FROM 
    device_category_user_metrics
ORDER BY 
    user_count DESC;
