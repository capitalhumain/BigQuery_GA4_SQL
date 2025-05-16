DECLARE start_date DATE DEFAULT '2024-01-01';
DECLARE end_date DATE DEFAULT '2024-12-31';

SELECT
    ecommerce.transaction_id AS transaction_id,  -- Unique transaction identifier
    SUM(ecommerce.total_item_quantity) AS total_item_quantity,  -- Total quantity of items in the transaction
    SUM(ecommerce.purchase_revenue_in_usd) AS purchase_revenue_usd,  -- Total revenue in USD from the transaction
    SUM(ecommerce.purchase_revenue) AS purchase_revenue,  -- Total revenue in the default currency
    SUM(ecommerce.refund_value_in_usd) AS refund_value_usd,  -- Total refund value in USD
    SUM(ecommerce.refund_value) AS refund_value,  -- Total refund value in the default currency
    SUM(ecommerce.shipping_value_in_usd) AS shipping_value_usd,  -- Shipping cost in USD
    SUM(ecommerce.shipping_value) AS shipping_value,  -- Shipping cost in the default currency
    SUM(ecommerce.tax_value_in_usd) AS tax_value_usd,  -- Tax value in USD
    SUM(ecommerce.tax_value) AS tax_value,  -- Tax value in the default currency
    SUM(ecommerce.unique_items) AS unique_items  -- Count of unique items in the transaction
FROM
    `your-project-id.analytics_1234567890.events_*`  -- Replace with your actual project and dataset
WHERE
    event_name = 'purchase'  -- Filter for purchase events
    AND event_date BETWEEN start_date AND end_date  -- Filter for the date range
GROUP BY
    ecommerce.transaction_id  -- Group by transaction ID to get statistics per transaction;
ORDER BY
    transaction_id;  -- Sort by transaction ID for consistency
