-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-08-01';
DECLARE end_date STRING DEFAULT '2024-08-31';

WITH user_events AS (
  -- Flatten event data for session and product analysis
  SELECT
    PARSE_TIMESTAMP('%F %T', FORMAT_TIMESTAMP('%F %T', TIMESTAMP_MICROS(event_timestamp), 'America/New_York')) AS event_timestamp,
    event_name,
    user_pseudo_id,
    CONCAT(collected_traffic_source.manual_source, ' / ', collected_traffic_source.manual_medium) AS traffic_source,
    collected_traffic_source.gclid,
    CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') AS session_engaged,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS engagement_time_msec,
    ecommerce.purchase_revenue
  -- Replace with your own project and dataset ID
  FROM `project.dataset.events_*`
  WHERE _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
),

session_data AS (
  -- Aggregate session-level data and handle Google Ads misattribution
  SELECT
    EXTRACT(DATE FROM event_timestamp) AS date,
    user_pseudo_id,
    session_id,
    CASE 
      WHEN gclid IS NOT NULL THEN 'google / cpc' 
      ELSE traffic_source 
    END AS traffic_source, 
    MIN(event_timestamp) OVER (PARTITION BY session_id) AS session_started_at,
    MAX(event_timestamp) OVER (PARTITION BY session_id) AS session_ended_at,
    MAX(session_engaged) OVER (PARTITION BY session_id) AS session_engaged,
    SUM(engagement_time_msec) OVER (PARTITION BY session_id) AS engagement_time_msec,
    COUNT(*) OVER (PARTITION BY session_id) AS event_count,
    COUNTIF(event_name = 'page_view') OVER (PARTITION BY session_id) AS pageview_count,
    COUNTIF(event_name = 'purchase') OVER (PARTITION BY session_id) AS purchases,
    SUM(purchase_revenue) OVER (PARTITION BY session_id) AS total_revenue
  FROM user_events
  QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_timestamp ASC) = 1
),

last_non_direct_source AS (
  -- Identify the last non-direct traffic source for each user
  SELECT
    *,
    LAST_VALUE(traffic_source IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY session_started_at) AS last_non_direct_source
  FROM session_data
),

session_products AS (
  -- Extract products purchased per session
  SELECT
    CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
    items.item_category,
    items.item_name,
    SUM(items.item_revenue) AS total_price,
    SUM(items.quantity) AS total_quantity
  -- Replace with your own project and dataset ID
  FROM `project.dataset.events_*`,
       UNNEST(items) AS items
  WHERE event_name = 'purchase' AND ecommerce.transaction_id LIKE '%live%'
  GROUP BY session_id, item_category, item_name
),

product_sessions_combined AS (
  -- Join session data with purchased products data
  SELECT
    lnd.*,
    sp.item_category,
    sp.item_name,
    sp.total_price,
    sp.total_quantity
  FROM last_non_direct_source lnd
  LEFT JOIN session_products sp ON lnd.session_id = sp.session_id
),

category_totals AS (
  -- Aggregate total values per product category
  SELECT
    item_category,
    item_name,
    SUM(total_quantity) AS total_products_purchased,
    ROUND(SUM(total_price), 2) AS total_category_revenue
  FROM session_products
  GROUP BY item_category, item_name
)

-- Final aggregation of purchases and revenue by traffic source and product category
SELECT
  IFNULL(pc.last_non_direct_source, 'direct') AS traffic_source,
  pc.item_category,
  pc.item_name,
  ct.total_products_purchased,
  ct.total_category_revenue,
  SUM(pc.total_quantity) AS total_quantity_purchased,
  ROUND(SUM(pc.total_quantity) / ct.total_products_purchased, 2) AS purchase_percentage_of_total,
  ROUND(SUM(pc.total_price), 2) AS total_revenue,
  ROUND(SUM(pc.total_price) / ct.total_category_revenue, 2) AS revenue_percentage_of_total
FROM product_sessions_combined pc
LEFT JOIN category_totals ct ON pc.item_category = ct.item_category AND pc.item_name = ct.item_name
GROUP BY traffic_source, pc.item_category, pc.item_name, ct.total_products_purchased, ct.total_category_revenue
ORDER BY total_category_revenue DESC;
