-- Declaration of date range for analysis
DECLARE start_date DATE DEFAULT '2024-01-19';
DECLARE end_date DATE DEFAULT '2024-12-31';

-- Custom function to transform URLs based on specified type
CREATE TEMP FUNCTION transform_url(page STRING, type STRING) AS (
  CASE
    WHEN type = 'parameters' THEN REGEXP_REPLACE(page, '\\?.*$|#.*', '')
    WHEN type = 'protocol' THEN REGEXP_REPLACE(page, '^(https?://|ftp://)', '')
    WHEN type = 'domain' THEN REGEXP_REPLACE(page, '^(https?://|ftp://)?[^/]+/', '/')
    WHEN type = 'all' THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(page, '\\?.*$|#.*', ''), '^(https?://|ftp://)', ''), '^[^/]+/', '/')
  END
);

-- CTE for extracting conversions from GA4 dataset
WITH conversions AS (
  SELECT
    user_pseudo_id,
    -- Extracting session ID
    (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id') AS ga_session_id,
    -- Calculating purchase value with a combination of potential value types
    (SELECT IFNULL(value.int_value, 0) FROM UNNEST(event_params) WHERE KEY = 'value') + 
    (SELECT IFNULL(value.float_value, 0.0) FROM UNNEST(event_params) WHERE KEY = 'value') + 
    (SELECT IFNULL(value.double_value, 0.0) FROM UNNEST(event_params) WHERE KEY = 'value') AS purchase_value
  FROM
    `GA4_table_id` -- Customize with your GA4 table ID
  WHERE
    event_name = 'purchase'
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
), 

-- CTE for extracting session starts filtered by organic traffic from Google
session_starts AS (
  SELECT
    user_pseudo_id,
    -- Extracting session ID
    (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id') AS ga_session_id,
    -- Extracting page location
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location') AS page_location
  FROM
    `GA4_table_id` -- Customize with your GA4 table ID
  WHERE
    event_name = 'session_start'
    AND concat (collected_traffic_source.manual_source,'/',collected_traffic_source.manual_medium) = 'google/organic'
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
),

-- CTE for merging sessions and conversions based on user ID and session ID
filtered_sessions AS (
  SELECT
    ss.user_pseudo_id,
    ss.ga_session_id,
    ss.page_location,
    sus.purchase_value
  FROM
    session_starts ss
  JOIN
    conversions sus
  ON
    ss.user_pseudo_id = sus.user_pseudo_id AND ss.ga_session_id = sus.ga_session_id
),

-- CTE for aggregating Google Analytics data by transformed page location
ga_data AS (
  SELECT
    transform_url(page_location, 'all') AS transformed_page_location,
    COUNT(*) AS num_sessions,
    SUM(purchase_value) AS total_purchase_value
  FROM
    filtered_sessions
  GROUP BY
    transformed_page_location
),

-- CTE for aggregating Google Search Console data by query and transformed URL
gsc_data AS (
  SELECT
    query AS search_query,
    transform_url(url, 'all') AS transformed_url,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks
  FROM
    `GSC_table_id` -- Customize with your GSC table ID
  WHERE
    clicks > 0
    AND data_date BETWEEN start_date AND end_date
    AND query IS NOT NULL
    AND url IS NOT NULL
  GROUP BY
    search_query,
    transformed_url
),

-- CTE for calculating percentage of total clicks and share of voice conversion
pot_calculation AS (
  SELECT
    gsc.search_query,
    gsc.transformed_url,
    gsc.total_impressions,
    gsc.total_clicks,
    ga.num_sessions,
    ga.total_purchase_value,
    ROUND(gsc.total_clicks / SUM(gsc.total_clicks) OVER (PARTITION BY gsc.transformed_url), 2) AS percent_of_total_clicks,
    ROUND(ga.num_sessions * gsc.total_clicks / SUM(gsc.total_clicks) OVER (PARTITION BY gsc.transformed_url), 2) AS sov_conversion
  FROM
    gsc_data gsc
  LEFT JOIN
    ga_data ga ON gsc.transformed_url = ga.transformed_page_location
  WHERE
    ga.num_sessions > 0
)

-- Final query aggregating the share of voice conversion and purchase value by search query
SELECT
  search_query,
  ROUND(SUM(sov_conversion),2) AS total_sov_conversion,
  ROUND(SUM(sov_conversion * total_purchase_value),2) AS total_purchase_value
FROM
  pot_calculation
GROUP BY
  search_query;
