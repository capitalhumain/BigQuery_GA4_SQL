-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH user_revenue_metrics AS (
    -- Extract maximum revenue for each user with positive lifetime revenue
    SELECT
        pseudo_user_id,
        MAX(user_ltv.revenue_in_usd) AS max_revenue -- Max lifetime revenue per user
    FROM
        `project.dataset.pseudonymous_users_*` -- Replace with your actual project and dataset ID
    WHERE
        _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Date range filter
    GROUP BY 
        pseudo_user_id
    HAVING 
        max_revenue > 0 -- Filter users with positive lifetime revenue
)

-- Calculate Average Revenue Per User (ARPU) for users with positive lifetime revenue
SELECT
    ROUND(SUM(max_revenue) / COUNT(DISTINCT pseudo_user_id), 2) AS arpu -- Average revenue per user with LTV > 0
FROM 
    user_revenue_metrics;
