DECLARE start_date DATE DEFAULT '2024-01-01'; -- Start date for filtering
DECLARE end_date DATE DEFAULT '2024-12-31'; -- End date for filtering

SELECT
  event_date,
  session_traffic_source_last_click.google_ads_campaign.campaign_name AS campaign_name, -- Google Ads Campaign Name
  SUM(ecommerce.purchase_revenue_in_usd) AS daily_revenue, -- Total Revenue from Purchases
  COUNT(DISTINCT ecommerce.transaction_id) AS daily_transactions, -- Count of Transactions
  COUNT(DISTINCT user_pseudo_id) AS unique_users -- Count of Unique Users
FROM
  `your-project-id.analytics_1234567890.events_*` -- Replace with your actual project and dataset
WHERE
  session_traffic_source_last_click.google_ads_campaign.campaign_name IS NOT NULL -- Filter valid campaigns
  AND ecommerce.transaction_id IS NOT NULL -- Ensure transaction ID exists
  AND  _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
GROUP BY
  event_date, campaign_name
ORDER BY
  event_date, daily_revenue DESC;
