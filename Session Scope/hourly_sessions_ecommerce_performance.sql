-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH sessions AS (
  -- Calculate the number of sessions grouped by event date and hour
  SELECT
    event_date,
    FORMAT('%02d', EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp))) AS hour,
    COUNT(DISTINCT params.value.int_value) AS sessions
  FROM
    `project.dataset.events_*`, -- Replace with your own project and dataset ID
    UNNEST(event_params) AS params
  WHERE
    event_name = 'session_start'
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
    AND params.key = 'ga_session_id'
  GROUP BY event_date, hour
),

pageviews AS (
  -- Count pageviews grouped by event date and hour
  SELECT
    event_date,
    FORMAT('%02d', EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp))) AS hour,
    COUNT(event_timestamp) AS pageviews
  FROM
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE
    event_name = 'page_view'
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
  GROUP BY event_date, hour
),

view_item_sessions AS (
  -- Count sessions with "view_item" events grouped by event date and hour
  SELECT
    event_date,
    FORMAT('%02d', EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp))) AS hour,
    COUNT(DISTINCT params.value.int_value) AS view_item_sessions
  FROM
    `project.dataset.events_*`, -- Replace with your own project and dataset ID
    UNNEST(event_params) AS params,
    UNNEST(items) AS items
  WHERE
    event_name = 'view_item'
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
    AND params.key = 'ga_session_id'
  GROUP BY event_date, hour
),

add_to_carts AS (
  -- Count sessions with "add_to_cart" events grouped by event date and hour
  SELECT
    event_date,
    FORMAT('%02d', EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp))) AS hour,
    COUNT(DISTINCT params.value.int_value) AS add_to_cart_sessions
  FROM
    `project.dataset.events_*`, -- Replace with your own project and dataset ID
    UNNEST(event_params) AS params,
    UNNEST(items) AS items
  WHERE
    event_name = 'add_to_cart'
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
    AND params.key = 'ga_session_id'
  GROUP BY event_date, hour
),

orders AS (
  -- Count sessions with "purchase" events grouped by event date and hour
  SELECT
    event_date,
    FORMAT('%02d', EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp))) AS hour,
    COUNT(DISTINCT params.value.int_value) AS order_sessions
  FROM
    `project.dataset.events_*`, -- Replace with your own project and dataset ID
    UNNEST(event_params) AS params,
    UNNEST(items) AS items
  WHERE
    event_name = 'purchase'
    AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
    AND params.key = 'ga_session_id'
  GROUP BY event_date, hour
)

-- Aggregate and combine data from all metrics
SELECT
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  FORMAT_DATE('%W', PARSE_DATE('%Y%m%d', event_date)) AS iso_week_of_the_year,
  FORMAT_DATE('%w - %A', PARSE_DATE('%Y%m%d', event_date)) AS weekday,
  s.hour,
  s.sessions,
  p.pageviews,
  vi.view_item_sessions,
  ac.add_to_cart_sessions,
  o.order_sessions
FROM
  sessions AS s
LEFT JOIN pageviews AS p USING (event_date, hour)
LEFT JOIN view_item_sessions AS vi USING (event_date, hour)
LEFT JOIN add_to_carts AS ac USING (event_date, hour)
LEFT JOIN orders AS o USING (event_date, hour)
ORDER BY
  weekday, s.hour;
