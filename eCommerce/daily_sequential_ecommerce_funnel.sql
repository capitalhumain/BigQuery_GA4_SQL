-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH events_data AS (
  -- Extract relevant event data
  SELECT
    user_pseudo_id,
    event_name,
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp
  FROM 
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE 
    event_name IN ('page_view', 'view_item', 'add_to_cart', 'begin_checkout', 'purchase')
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
),

event_stages AS (
  -- Map event names to funnel stages
  SELECT
    user_pseudo_id,
    event_date,
    event_timestamp,
    CASE event_name
      WHEN 'page_view' THEN 'page_view'
      WHEN 'view_item' THEN 'view_item'
      WHEN 'add_to_cart' THEN 'add_to_cart'
      WHEN 'begin_checkout' THEN 'begin_checkout'
      WHEN 'purchase' THEN 'purchase'
    END AS event_stage
  FROM 
    events_data
),

aggregated_funnel AS (
  -- Calculate funnel metrics by stage
  SELECT
    pv.event_date,
    COUNT(DISTINCT pv.user_pseudo_id) AS page_view_count,
    COUNT(DISTINCT vi.user_pseudo_id) AS view_item_count,
    COUNT(DISTINCT atc.user_pseudo_id) AS add_to_cart_count,
    COUNT(DISTINCT bc.user_pseudo_id) AS begin_checkout_count,
    COUNT(DISTINCT p.user_pseudo_id) AS purchase_count
  FROM 
    event_stages pv
    LEFT JOIN event_stages vi
      ON pv.user_pseudo_id = vi.user_pseudo_id
      AND pv.event_date = vi.event_date
      AND pv.event_timestamp <= vi.event_timestamp
      AND vi.event_stage = 'view_item'
    LEFT JOIN event_stages atc
      ON vi.user_pseudo_id = atc.user_pseudo_id
      AND vi.event_date = atc.event_date
      AND vi.event_timestamp <= atc.event_timestamp
      AND atc.event_stage = 'add_to_cart'
    LEFT JOIN event_stages bc
      ON atc.user_pseudo_id = bc.user_pseudo_id
      AND atc.event_date = bc.event_date
      AND atc.event_timestamp <= bc.event_timestamp
      AND bc.event_stage = 'begin_checkout'
    LEFT JOIN event_stages p
      ON bc.user_pseudo_id = p.user_pseudo_id
      AND bc.event_date = p.event_date
      AND bc.event_timestamp <= p.event_timestamp
      AND p.event_stage = 'purchase'
  WHERE 
    pv.event_stage = 'page_view'
  GROUP BY 
    pv.event_date
)

-- Calculate conversion rates and output results
SELECT
  event_date,
  page_view_count AS page_view,
  view_item_count AS view_item,
  add_to_cart_count AS add_to_cart,
  begin_checkout_count AS begin_checkout,
  purchase_count AS purchase,
  -- Conversion rates between stages
  ROUND(COALESCE(view_item_count / NULLIF(page_view_count, 0), 0) * 100, 2) AS view_item_rate, -- View Item/Page View as %
  ROUND(COALESCE(add_to_cart_count / NULLIF(view_item_count, 0), 0) * 100, 2) AS add_to_cart_rate, -- Add to Cart/View Item as %
  ROUND(COALESCE(begin_checkout_count / NULLIF(view_item_count, 0), 0) * 100, 2) AS begin_checkout_rate, -- Begin Checkout/View Item as %
  ROUND(COALESCE(purchase_count / NULLIF(view_item_count, 0), 0) * 100, 2) AS purchase_rate -- Purchase/View Item as %
FROM 
  aggregated_funnel
ORDER BY 
  event_date ASC;
