-- Declare date range for filtering jobs (last 90 days)
DECLARE start_date STRING DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);

WITH load_job_metrics AS (
    -- Extract metrics for load jobs related to event tables
    SELECT
        FORMAT_DATE('%F', creation_time) AS creation_date, -- Extract the creation date
        AVG(EXTRACT(HOUR FROM creation_time) * 60 + EXTRACT(MINUTE FROM creation_time)) 
            OVER (ORDER BY FORMAT_DATE('%F', creation_time) ASC) AS avg_refresh_time_minutes, -- Average refresh time in minutes
        MIN(FORMAT_TIMESTAMP('%R', creation_time)) AS earliest_creation_time_utc, -- Earliest job creation time (UTC)
        destination_table.table_id AS table_id, -- Target table ID
        total_bytes_processed, -- Bytes processed by the job
        COUNT(*) AS load_job_count -- Total number of load jobs
    FROM 
        `region-us`.INFORMATION_SCHEMA.JOBS -- Replace with correct region
    WHERE
        start_time > TIMESTAMP(start_date) -- Filter jobs from the last 90 days
        AND job_type = 'LOAD' -- Filter for load jobs only
        AND destination_table.dataset_id = 'analytics_1234567' -- Replace with actual dataset
        AND destination_table.table_id LIKE '%events_%' -- Filter for event tables
    GROUP BY 
        destination_table.table_id, total_bytes_processed, creation_time
    HAVING 
        total_bytes_processed > 1000 -- Filter jobs with significant data processed
)

-- Final query to select relevant metrics and order by creation date
SELECT
    creation_date,
    FORMAT_TIMESTAMP('%H:%M', TIMESTAMP_SECONDS(CAST(avg_refresh_time_minutes * 60 AS INT64))) AS avg_refresh_time_utc, -- Average refresh time (UTC)
    earliest_creation_time_utc,
    table_id,
    total_bytes_processed,
    load_job_count
FROM 
    load_job_metrics
ORDER BY 
    creation_date DESC; -- Order by most recent creation date
