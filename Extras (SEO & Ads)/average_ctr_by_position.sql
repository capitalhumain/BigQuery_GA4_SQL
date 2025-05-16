DECLARE start_date DATE DEFAULT '2024-01-01'; -- Replace with your desired start date
DECLARE end_date DATE DEFAULT '2024-12-31'; -- Replace with your desired end date

WITH RawMetrics AS (
  -- Calculate position and average CTR
  SELECT
    ((SUM(sum_top_position) / SUM(impressions)) + 1.0) AS raw_position, -- Raw position calculation
    SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS avg_ctr -- Calculate average CTR
  FROM
    `your-project-id.dataset_name.searchdata_site_impression` -- Replace with your actual project and dataset
  WHERE
    impressions > 0 -- Consider only rows with impressions
    AND sum_top_position < 20 -- Focus on valid top positions
    AND data_date BETWEEN start_date AND end_date -- Filter by declared date range
  GROUP BY
    query
),
PositionMetrics AS (
  SELECT
    ROUND(raw_position) AS position, -- Round raw position to the nearest integer
    AVG(avg_ctr) AS avg_ctr -- Aggregate average CTR by integer position
  FROM
    RawMetrics
  GROUP BY
    ROUND(raw_position) -- Group by rounded position
)

-- Final Output
SELECT
  position,
  ROUND(avg_ctr * 100, 2) AS avg_ctr_percentage -- Convert CTR to a percentage for readability
FROM
  PositionMetrics
ORDER BY
  position;
