-- Declare date range variables to filter data by date
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH pseudonymous_user_metrics AS (
    -- Extract maximum lifetime value (LTV) and session count for each pseudonymous user
    SELECT
        pseudo_user_id,
        MAX(user_ltv.revenue_in_usd) AS user_ltv, -- Maximum lifetime revenue for the user
        MAX(user_ltv.sessions) AS total_sessions -- Maximum session count for the user
    FROM
        `project.dataset.pseudonymous_users_*` -- Replace with your actual project and dataset ID
    WHERE
        user_ltv.revenue_in_usd > 0 -- Filter for users with positive lifetime revenue
        AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Date range filter
    GROUP BY 
        pseudo_user_id
)

-- Final query to get LTV and session metrics for pseudonymous users
SELECT
    pseudo_user_id,
    user_ltv,
    total_sessions
FROM 
    pseudonymous_user_metrics
ORDER BY 
    user_ltv DESC; -- Order by highest lifetime value, descending
