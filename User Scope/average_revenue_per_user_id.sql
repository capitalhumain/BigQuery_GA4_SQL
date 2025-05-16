-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH user_revenue_metrics AS (
    -- Extract maximum revenue for each user
    SELECT
        user_id,
        MAX(user_ltv.revenue_in_usd) AS max_revenue -- Max lifetime revenue for each user
    FROM
        `project.dataset.users_*` -- Replace with your actual project and dataset ID
    WHERE
        _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Date range filter
    GROUP BY 
        user_id
)

-- Calculate Average Revenue Per User (ARPU)
SELECT
    ROUND(SUM(max_revenue) / COUNT(DISTINCT user_id), 2) AS arpu -- Average revenue per user
FROM 
    user_revenue_metrics;
