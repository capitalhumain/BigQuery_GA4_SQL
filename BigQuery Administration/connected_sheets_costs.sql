DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY); -- Start date for cost tracking
DECLARE end_date DATE DEFAULT CURRENT_DATE(); -- End date for cost tracking
DECLARE timezone STRING DEFAULT "UTC"; -- Set time zone
DECLARE gb_divisor INT64 DEFAULT 1024*1024*1024; -- Convert bytes to GB
DECLARE tb_divisor INT64 DEFAULT gb_divisor * 1024; -- Convert GB to TB
DECLARE cost_per_tb_in_dollar FLOAT64 DEFAULT 6.25; -- Cost per TB
DECLARE cost_factor FLOAT64 DEFAULT cost_per_tb_in_dollar / tb_divisor; -- Cost factor for bytes

WITH QueryCosts AS (
  SELECT
    labels.key AS label_key,  -- Label key for filtering
    labels.value AS label_value,  -- Label value for filtering
    DATE(creation_time, timezone) AS creation_date,  -- Date of query execution
    FORMAT_TIMESTAMP("%F %H:%I:%S", creation_time, timezone) AS query_time,  -- Formatted timestamp
    job_id,  -- Job identifier for the query
    ROUND(total_bytes_processed / gb_divisor, 2) AS bytes_processed_in_gb,  -- Bytes processed in GB
    IF(cache_hit != TRUE, ROUND(total_bytes_processed * cost_factor, 4), 0) AS cost_in_dollar,  -- Cost in dollars (only if not cached)
    project_id,  -- Project ID of the job
    user_email  -- User who executed the query
  FROM 
    `your-region-here`.INFORMATION_SCHEMA.JOBS_BY_PROJECT,  -- Replace with your actual region
    UNNEST(labels) AS labels  -- Unnest labels to extract key-value pairs
  WHERE
    DATE(creation_time) BETWEEN start_date AND end_date  -- Filter by date range
    AND job_type = 'QUERY'  -- Filter for query jobs
    AND labels.value = 'connected_sheets'  -- Filter by connected sheets label
  ORDER BY
    bytes_processed_in_gb DESC  -- Order by the amount of data processed
)

-- Calculate total costs for each day within the date range
SELECT
  creation_date,  -- Date of the query
  SUM(cost_in_dollar) AS total_cost  -- Total cost for the day
FROM
  QueryCosts
GROUP BY
  creation_date
ORDER BY
  creation_date ASC;  -- Order by date
