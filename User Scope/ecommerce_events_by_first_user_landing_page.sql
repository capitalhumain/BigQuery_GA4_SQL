 -- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

WITH first_page_view_locations AS (
  -- Capture the first page viewed in the session for each user
  SELECT
    user_pseudo_id,
    REGEXP_REPLACE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'\?.*$', '') AS first_pageview_location,
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ASC) AS row_num
-- Replace with your own project and dataset ID
  FROM `project.dataset.events_*`
  WHERE event_name = 'page_view'
    AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') = 1
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
  QUALIFY row_num = 1
),

key_events AS (
  -- Gather key events and calculate purchase value per user and date
  SELECT
    user_pseudo_id,
    event_name,
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    COUNT(1) AS event_count,
    SUM(CASE WHEN event_name = 'purchase' THEN event_value_in_usd ELSE 0 END) AS purchase_value
-- Replace with your own project and dataset ID
  FROM `project.dataset.events_*`
  WHERE event_name IN ('view_item', 'email_signup', 'view_item_list', 'add_to_cart', 'select_item', 'begin_checkout', 'add_shipping_info', 'purchase')
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
  GROUP BY user_pseudo_id, event_name, event_date
)

-- Aggregate key event counts and purchase values by page location and date
SELECT
  fpl.first_pageview_location AS page_location,
  fpl.event_date,
  COUNT(IF(ke.event_name = 'view_item', 1, NULL)) AS view_item_count,
  COUNT(IF(ke.event_name = 'view_item_list', 1, NULL)) AS view_item_list_count,
  COUNT(IF(ke.event_name = 'add_to_cart', 1, NULL)) AS add_to_cart_count,
  COUNT(IF(ke.event_name = 'select_item', 1, NULL)) AS select_item_count,
  COUNT(IF(ke.event_name = 'begin_checkout', 1, NULL)) AS begin_checkout_count,
  COUNT(IF(ke.event_name = 'add_shipping_info', 1, NULL)) AS add_shipping_info_count,
  COUNT(IF(ke.event_name = 'purchase', 1, NULL)) AS purchase_count,
  SUM(ke.purchase_value) AS total_purchase_value
FROM key_events ke
JOIN first_page_view_locations fpl
  ON ke.user_pseudo_id = fpl.user_pseudo_id
GROUP BY fpl.first_pageview_location, fpl.event_date
ORDER BY total_purchase_value DESC, fpl.first_pageview_location, fpl.event_date;
