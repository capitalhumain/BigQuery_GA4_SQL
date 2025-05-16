DECLARE start_date DATE DEFAULT '2024-01-01';
DECLARE end_date DATE DEFAULT '2024-12-31';

SELECT
    event_date AS date,  -- Date of the event
    COUNT(DISTINCT ecommerce.transaction_id) AS transactions,  -- Number of distinct transactions
    SUM(ecommerce.purchase_revenue) AS purchase_revenue  -- Total revenue from purchases
FROM
    `your-project-id.analytics_1234567890.events_*`  -- Replace with your project and dataset ID
WHERE
    event_date BETWEEN start_date AND end_date  -- Filter by date range
    AND ecommerce.transaction_id IS NOT NULL  -- Ensure transactions are valid
GROUP BY 
    event_date  -- Group by event date
ORDER BY 
    event_date;  -- Sort by date for chronological order
