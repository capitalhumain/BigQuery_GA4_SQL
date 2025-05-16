-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH lifetime_user_metrics AS (
    -- Extract maximum lifetime value (CLV) and total sessions for each user
    SELECT
        pseudo_user_id,
        MAX(user_ltv.revenue_in_usd) AS max_revenue_in_usd, -- Max lifetime revenue for each user
        MAX(user_ltv.sessions) AS total_sessions -- Max sessions per user
    FROM 
        `project.dataset.pseudonymous_users_*` -- Replace with your own project and dataset ID
    WHERE
        user_ltv.revenue_in_usd > 0 -- Filter users with positive lifetime revenue
        AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Date range filter
    GROUP BY 
        pseudo_user_id
)

-- Calculate correlation between lifetime value (CLV) and total sessions
SELECT
    ROUND(CORR(max_revenue_in_usd, total_sessions), 2) AS clv_sessions_correlation -- Correlation between CLV and sessions
FROM 
    lifetime_user_metrics;
