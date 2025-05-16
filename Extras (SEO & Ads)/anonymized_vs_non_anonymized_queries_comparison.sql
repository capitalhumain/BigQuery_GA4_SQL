-- Declare date range
DECLARE start_date DATE DEFAULT '2024-01-01'; -- Replace with your desired start date
DECLARE end_date DATE DEFAULT '2024-12-31'; -- Replace with your desired end date

WITH QueryMetrics AS (
  -- Calculate total metrics for anonymized and non-anonymized queries
  SELECT
    data_date,
    SUM(CASE WHEN is_anonymized_query = TRUE THEN clicks ELSE 0 END) AS anonymized_clicks, -- Total clicks for anonymized queries
    SUM(CASE WHEN is_anonymized_query = TRUE THEN impressions ELSE 0 END) AS anonymized_impressions, -- Total impressions for anonymized queries
    SUM(CASE WHEN is_anonymized_query = FALSE THEN clicks ELSE 0 END) AS non_anonymized_clicks, -- Total clicks for non-anonymized queries
    SUM(CASE WHEN is_anonymized_query = FALSE THEN impressions ELSE 0 END) AS non_anonymized_impressions, -- Total impressions for non-anonymized queries
    COUNTIF(is_anonymized_query = TRUE) AS anonymized_query_count, -- Total number of anonymized queries
    COUNTIF(is_anonymized_query = FALSE) AS non_anonymized_query_count, -- Total number of non-anonymized queries
    COUNT(*) AS total_query_count, -- Total query count
    SUM(clicks) AS total_clicks, -- Total clicks across all queries
    SUM(impressions) AS total_impressions -- Total impressions across all queries
  FROM 
    `your-project-id.dataset_name.searchdata_site_impression` -- Replace with your actual project and dataset
  WHERE 
    DATE(data_date) BETWEEN start_date AND end_date -- Filter by declared date range
  GROUP BY 
    data_date
),
DailyPercentages AS (
  -- Calculate percentages and aggregate metrics
  SELECT
    data_date,
    anonymized_query_count,
    non_anonymized_query_count,
    ROUND(SAFE_DIVIDE(anonymized_query_count, total_query_count) * 100, 2) AS anonymized_query_percentage, -- Percentage of anonymized queries
    ROUND(SAFE_DIVIDE(non_anonymized_query_count, total_query_count) * 100, 2) AS non_anonymized_query_percentage, -- Percentage of non-anonymized queries
    anonymized_clicks,
    anonymized_impressions,
    non_anonymized_clicks,
    non_anonymized_impressions,
    total_clicks,
    total_impressions
  FROM 
    QueryMetrics
)

-- Final output
SELECT
  data_date,
  anonymized_query_percentage, -- Percentage of anonymized queries
  anonymized_clicks, -- Total clicks for anonymized queries
  anonymized_impressions, -- Total impressions for anonymized queries
  non_anonymized_query_percentage, -- Percentage of non-anonymized queries
  non_anonymized_clicks, -- Total clicks for non-anonymized queries
  non_anonymized_impressions, -- Total impressions for non-anonymized queries
  total_clicks, -- Total clicks across all queries
  total_impressions -- Total impressions across all queries
FROM 
  DailyPercentages
ORDER BY 
  data_date;
