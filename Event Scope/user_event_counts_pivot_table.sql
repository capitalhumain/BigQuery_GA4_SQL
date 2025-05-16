-- Declare date range variables to filter data for the year 2024
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

SELECT
    *
FROM (
    SELECT
        user_pseudo_id,
        event_name
  -- Replace with your own project and dataset ID
    FROM `project.dataset.events_*`
    WHERE _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
)
PIVOT (
    COUNT(*)
    FOR event_name IN (
        'session_start', 
        'page_view',
        'signup_success',
        'login_success',
        'whitelabel_final_step',
        'upgrade', 
        'cancellation',
        'purchase',
        'contact_telegram'
    )
)
ORDER BY user_pseudo_id;
