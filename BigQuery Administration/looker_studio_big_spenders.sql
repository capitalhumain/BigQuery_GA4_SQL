DECLARE gb_divisor INT64 DEFAULT 1024 * 1024 * 1024;  
DECLARE cost_per_tb FLOAT64 DEFAULT 6.25;  
DECLARE cost_factor FLOAT64 DEFAULT cost_per_tb / (gb_divisor * 1024);  
DECLARE start_date DATE DEFAULT '2024-01-01';  -- Set your start date here
DECLARE end_date DATE DEFAULT CURRENT_DATE();  -- Set end date or use current date

WITH LookerStudioJobs AS (
  SELECT
    job_id,
    DATE(creation_time) AS creation_date,
    FORMAT_TIMESTAMP("%F %H:%M:%S", creation_time) AS creation_time,
    user_email AS datasource_owner,
    LEFT((SELECT l.value FROM UNNEST(labels) l WHERE l.key = "looker_studio_datasource_id"), 36) AS datasource_id,
    LEFT((SELECT l.value FROM UNNEST(labels) l WHERE l.key = "looker_studio_report_id"), 36) AS report_id,
    ROUND(total_bytes_processed / gb_divisor, 2) AS processed_bytes_gb,
    IF(cache_hit != TRUE, ROUND(total_bytes_processed * cost_factor, 4), 0) AS cost_usd
  FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT -- Replace with your actual region
  WHERE
    (SELECT l.value FROM UNNEST(labels) l WHERE l.key = "requestor") = "looker_studio"
    AND DATE(creation_time) BETWEEN start_date AND end_date  -- Apply date filter
),

AggregatedData AS (
  SELECT
    creation_date,
    datasource_owner,
    CONCAT("https://lookerstudio.google.com/datasources/", datasource_id) AS datasource_url,
    CONCAT("https://lookerstudio.google.com/reporting/", report_id) AS report_url,
    COUNT(DISTINCT job_id) AS total_jobs,
    SUM(processed_bytes_gb) AS total_data_gb,
    SUM(cost_usd) AS total_cost_usd
  FROM
    LookerStudioJobs
  WHERE
    datasource_id IS NOT NULL OR report_id IS NOT NULL
  GROUP BY
    creation_date, datasource_owner, datasource_url, report_url
),

MostSpenderUsers AS (
  -- Rank the users based on total spending
  SELECT
    datasource_owner,
    SUM(total_cost_usd) AS total_spend_usd,
    RANK() OVER (ORDER BY SUM(total_cost_usd) DESC) AS spend_rank
  FROM
    AggregatedData
  GROUP BY
    datasource_owner
)

SELECT
  ad.creation_date,
  ad.datasource_owner,
  ROUND(ad.total_data_gb, 2) AS total_processed_gb,
  ROUND(ad.total_cost_usd, 2) AS total_cost_usd,
  ms.total_spend_usd,
  ms.spend_rank
FROM
  AggregatedData ad
JOIN
  MostSpenderUsers ms
ON 
  ad.datasource_owner = ms.datasource_owner
ORDER BY
  ms.spend_rank ASC, ad.total_cost_usd DESC;  -- Sort by spend rank and cost
