DECLARE start_date DATE DEFAULT '2024-01-01'; -- Set your start date
DECLARE end_date DATE DEFAULT '2024-12-31'; -- Set your end date

WITH DailyMetrics AS (
  -- Calculate CTR and extract necessary metrics
  SELECT
    data_date,
    query,
    SUM(clicks) / SUM(impressions) AS ctr, -- Compute CTR
    ((SUM(sum_top_position) / SUM(impressions)) + 1.0) AS avg_position -- Calculate average position
  FROM 
    `your-project-id.your-dataset-id.searchdata_site_impression` -- Replace with your project and dataset
  WHERE
    impressions > 0 
    AND sum_top_position < 20 -- Only consider queries with impressions and reasonable positions
    AND data_date BETWEEN start_date AND end_date -- Apply date filters
  GROUP BY 
    data_date, query
),

DailyCorrelation AS (
  -- Compute correlation between CTR and average position for each day
  SELECT
    data_date,
    CORR(ctr, avg_position) AS ctr_position_correlation -- Correlation between CTR and average position
  FROM
    DailyMetrics
  GROUP BY
    data_date
)

SELECT 
  data_date,
  ctr_position_correlation
FROM 
  DailyCorrelation
ORDER BY 
  data_date;
