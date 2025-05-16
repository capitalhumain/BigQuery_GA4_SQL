DECLARE start_date STRING DEFAULT "2024-01-01"; -- Start date for filtering
DECLARE end_date STRING DEFAULT "2024-12-31";   -- End date for filtering

SELECT
  query,
  device,
  SUM(impressions) AS impressions, -- Total impressions
  SUM(clicks) AS clicks,           -- Total clicks
  ROUND(SUM(clicks) / SUM(impressions), 4) AS ctr, -- Click-through rate (CTR) rounded to 4 decimals
  ROUND(((SUM(sum_top_position) / SUM(impressions)) + 1.0), 2) AS avg_position -- Average position rounded to 2 decimals
FROM 
  `your-project-id.dataset-id.searchdata_site_impression` -- Replace with your project and dataset
WHERE
  DATE(data_date) BETWEEN DATE(start_date) AND DATE(end_date) -- Filter using declared date range
  AND query IS NOT NULL -- Exclude null queries
GROUP BY
  query, device
ORDER BY
  impressions DESC -- Order by impressions in descending order

