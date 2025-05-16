-- Declare pricing constants for different storage types (logical and physical)
DECLARE active_logical_gib_price FLOAT64 DEFAULT 0.02;
DECLARE long_term_logical_gib_price FLOAT64 DEFAULT 0.01;
DECLARE active_physical_gib_price FLOAT64 DEFAULT 0.04;
DECLARE long_term_physical_gib_price FLOAT64 DEFAULT 0.02;

WITH storage_sizes AS (
    -- Extract storage metrics by dataset and calculate the size of logical and physical storage
    SELECT
        table_schema AS dataset_name,
        -- Logical storage
        SUM(active_logical_bytes) / POWER(1024, 3) AS active_logical_gib,
        SUM(long_term_logical_bytes) / POWER(1024, 3) AS long_term_logical_gib,
        -- Physical storage
        SUM(active_physical_bytes) / POWER(1024, 3) AS active_physical_gib,
        SUM(active_physical_bytes - time_travel_physical_bytes - fail_safe_physical_bytes) / POWER(1024, 3) AS active_no_tt_no_fs_physical_gib,
        SUM(long_term_physical_bytes) / POWER(1024, 3) AS long_term_physical_gib,
        -- Restorable (previously deleted) physical storage
        SUM(time_travel_physical_bytes) / POWER(1024, 3) AS time_travel_physical_gib,
        SUM(fail_safe_physical_bytes) / POWER(1024, 3) AS fail_safe_physical_gib
    FROM
     `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT -- Replace with correct region
    WHERE 
        total_logical_bytes > 0
        AND total_physical_bytes > 0
        -- Only base the forecast on base tables for highest precision results
        AND table_type = 'BASE TABLE'
    GROUP BY 
        dataset_name
)

SELECT
    dataset_name,
    -- Logical storage cost calculation
    ROUND(
        ROUND(active_logical_gib * active_logical_gib_price, 2) + 
        ROUND(long_term_logical_gib * long_term_logical_gib_price, 2), 2
    ) AS total_logical_cost,
    -- Physical storage cost calculation
    ROUND(
        ROUND(active_physical_gib * active_physical_gib_price, 2) + 
        ROUND(long_term_physical_gib * long_term_physical_gib_price, 2), 2
    ) AS total_physical_cost
FROM
    storage_sizes;
