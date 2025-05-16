DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY); -- Start date for filtering
DECLARE end_date DATE DEFAULT CURRENT_DATE(); -- End date for filtering
DECLARE timezone STRING DEFAULT "UTC"; -- Timezone for formatting
DECLARE gb_divisor INT64 DEFAULT 1024 * 1024 * 1024; -- Convert bytes to GB
DECLARE tb_divisor INT64 DEFAULT gb_divisor * 1024; -- Convert GB to TB
DECLARE cost_per_tb_in_dollar FLOAT64 DEFAULT 6.25; -- Cost per TB
DECLARE cost_factor FLOAT64 DEFAULT cost_per_tb_in_dollar / tb_divisor; -- Cost factor for bytes

WITH LookerStudioJobInfo AS (
  SELECT
    job_id, -- Unique job identifier
    DATE(creation_time, timezone) AS creation_date, -- Query creation date
    FORMAT_TIMESTAMP("%F %H:%M:%S", creation_time, timezone) AS creation_time_local, -- Localized query creation time
    user_email AS datasource_owner, -- Owner of the datasource
    LEFT((SELECT l.value FROM UNNEST(labels) l WHERE l.key = "looker_studio_datasource_id"), 36) AS looker_studio_datasource_id, -- Looker Studio datasource ID
    LEFT((SELECT l.value FROM UNNEST(labels) l WHERE l.key = "looker_studio_report_id"), 36) AS looker_studio_report_id, -- Looker Studio report ID
    ROUND(total_bytes_processed / gb_divisor, 2) AS bytes_processed_gb, -- Data processed in GB
    IF(cache_hit != TRUE, ROUND(total_bytes_processed * cost_factor, 4), 0) AS cost_in_dollar -- Cost in dollars (excluding cached queries)
  FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT -- Replace with the actual region
  WHERE
    (SELECT l.value FROM UNNEST(labels) l WHERE l.key = "requestor") = "looker_studio" -- Filter for Looker Studio jobs
    AND DATE(creation_time) BETWEEN start_date AND end_date -- Filter by date range
),

AggregatedReportData AS (
  SELECT
    creation_date, -- Date of the query
    looker_studio_report_id, -- Report ID for Looker Studio
    CONCAT("https://lookerstudio.google.com/reporting/", looker_studio_report_id) AS looker_studio_report_url, -- Full report URL
    COUNT(DISTINCT job_id) AS total_jobs, -- Total number of jobs
    SUM(bytes_processed_gb) AS total_data_processed_gb, -- Total data processed in GB
    SUM(cost_in_dollar) AS total_cost_usd -- Total cost in USD
  FROM
    LookerStudioJobInfo
  WHERE
    looker_studio_report_id IS NOT NULL -- Ensure valid report IDs
  GROUP BY
    creation_date, looker_studio_report_id -- Group by creation date and report ID
)

SELECT
  creation_date, -- Query creation date
  looker_studio_report_url, -- Full Looker Studio report URL
  total_jobs, -- Total number of jobs
  ROUND(total_data_processed_gb, 2) AS total_data_processed_gb, -- Rounded total data processed in GB
  ROUND(total_cost_usd, 2) AS total_cost_usd -- Rounded total cost in USD
FROM
  AggregatedReportData
ORDER BY
  creation_date DESC, -- Sort by creation date (descending)
  total_cost_usd DESC; -- Sort by cost in USD (descending)
