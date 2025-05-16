-- Declare date range variables to filter data for the year 2024
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH user_events AS (
  -- Flattening event data for consistent structure
  SELECT
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    event_name,
    user_pseudo_id,
    user_id,
    CONCAT(traffic_source.source, ' / ', traffic_source.medium) AS traffic_source, 
    CONCAT(user_pseudo_id, '-', (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_id')) AS session_id,
    (
      SELECT value.int_value
      FROM UNNEST(event_params)
      WHERE key = 'ga_session_number'
    ) AS session_number,
    ecommerce.purchase_revenue AS transaction_value
  -- Replace with your own project and dataset ID
  FROM `project.dataset.events_*`
  WHERE _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
),

first_purchase_event AS (
  -- Identifying the first purchase traffic source for each user
  SELECT
    user_id,
    traffic_source AS first_traffic_source,
    ROW_NUMBER() OVER (
      PARTITION BY user_id 
      ORDER BY event_timestamp ASC
    ) AS rn
  FROM user_events
  WHERE event_name = 'purchase'
  QUALIFY rn = 1
),

user_purchase_aggregates AS (
  -- Calculating total purchases and revenue per user
  SELECT
    user_id,
    SUM(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
    SUM(CASE WHEN event_name = 'purchase' THEN transaction_value ELSE 0 END) AS total_revenue
  FROM user_events
  WHERE event_name = 'purchase'
  GROUP BY user_id
)

-- Combining first purchase source with user aggregates
SELECT
  IFNULL(fpe.first_traffic_source, '(direct) / (none)') AS first_source_medium,
  fpe.user_id,
  MAX(upa.total_purchases) AS total_purchases,
  MAX(ROUND(IFNULL(upa.total_revenue, 0), 2)) AS total_revenue
FROM first_purchase_event fpe
JOIN user_purchase_aggregates upa
  ON fpe.user_id = upa.user_id
GROUP BY first_source_medium, fpe.user_id
ORDER BY total_revenue DESC;


