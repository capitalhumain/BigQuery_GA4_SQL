-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH events_data AS (
  -- Extract event data and item details
  SELECT
    user_pseudo_id,
    event_name,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    item.item_name AS item_name 
  FROM 
    `project.dataset.events_*`, -- Replace with your own project and dataset ID
    UNNEST(items) AS item
  WHERE 
    event_name IN ('view_item', 'add_to_cart', 'begin_checkout', 'purchase')
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
),

event_stages AS (
  -- Map event names to their funnel stages
  SELECT
    user_pseudo_id,
    event_timestamp,
    item_name,
    CASE event_name
      WHEN 'view_item' THEN 'view_item'
      WHEN 'add_to_cart' THEN 'add_to_cart'
      WHEN 'begin_checkout' THEN 'begin_checkout'
      WHEN 'purchase' THEN 'purchase'
    END AS event_stage
  FROM 
    events_data
),

aggregated_funnel AS (
  -- Aggregate funnel metrics by item
  SELECT
    vi.item_name,
    COUNT(DISTINCT vi.user_pseudo_id) AS view_item_count,
    COUNT(DISTINCT atc.user_pseudo_id) AS add_to_cart_count,
    COUNT(DISTINCT bc.user_pseudo_id) AS begin_checkout_count,
    COUNT(DISTINCT p.user_pseudo_id) AS purchase_count
  FROM 
    event_stages vi
    LEFT JOIN event_stages atc
      ON vi.user_pseudo_id = atc.user_pseudo_id
      AND vi.item_name = atc.item_name
      AND vi.event_timestamp < atc.event_timestamp
      AND atc.event_stage = 'add_to_cart'
    LEFT JOIN event_stages bc
      ON atc.user_pseudo_id = bc.user_pseudo_id
      AND atc.item_name = bc.item_name
      AND atc.event_timestamp < bc.event_timestamp
      AND bc.event_stage = 'begin_checkout'
    LEFT JOIN event_stages p
      ON bc.user_pseudo_id = p.user_pseudo_id
      AND bc.item_name = p.item_name
      AND bc.event_timestamp < p.event_timestamp
      AND p.event_stage = 'purchase'
  WHERE 
    vi.event_stage = 'view_item'
  GROUP BY 
    vi.item_name
)

-- Calculate funnel conversion rates and output results
SELECT
  item_name,
  add_to_cart_count AS add_to_cart,
  begin_checkout_count AS begin_checkout,
  purchase_count AS purchase,
  ROUND(COALESCE(add_to_cart_count / NULLIF(view_item_count, 0), 0), 2) AS add_to_cart_rate, -- Add to Cart/View Item
  ROUND(COALESCE(begin_checkout_count / NULLIF(view_item_count, 0), 0), 2) AS begin_checkout_rate, -- Begin Checkout/View Item
  ROUND(COALESCE(purchase_count / NULLIF(view_item_count, 0), 0), 2) AS purchase_rate -- Purchase/View Item
FROM 
  aggregated_funnel
ORDER BY 
  purchase_count DESC; -- Sort by purchase count for insights
