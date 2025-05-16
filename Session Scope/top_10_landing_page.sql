-- Declare date range variables for filtering data
DECLARE start_date STRING DEFAULT '2024-01-01';  -- Replace with desired start date
DECLARE end_date STRING DEFAULT '2024-12-31';    -- Replace with desired end date

WITH LandingPageData AS (
    -- Extract relevant data for landing pages
    SELECT
        -- Clean and extract the page path without query parameters or fragments
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                (SELECT ep.value.string_value 
                 FROM UNNEST(e.event_params) AS ep 
                 WHERE ep.key = 'page_location'),
                r'(\?.*)$', '' -- Remove query parameters
            ),
            r'#.*$', '' 
        ) AS landing_page,
        -- Determine if this is a landing page
        CASE 
            WHEN (SELECT ep.value.int_value 
                  FROM UNNEST(e.event_params) AS ep 
                  WHERE ep.key = 'entrances') = 1 THEN 1
            ELSE 0 
        END AS is_landing_page,
        e.user_pseudo_id
    FROM 
        `project.dataset.events_*` AS e  -- Replace with your project and dataset
    WHERE 
        e.event_name = 'page_view'
        AND e.user_pseudo_id IS NOT NULL
        AND e.event_date BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')  -- Filter by date range
),

AggregatedLandingPages AS (
    -- Aggregate landing page data
    SELECT
        landing_page,
        COUNT(DISTINCT user_pseudo_id) AS unique_users, -- Count of unique users
        COUNT(*) AS page_views, -- Total page views
        SUM(is_landing_page) AS entrances, -- Total number of entrances
        ROUND(SAFE_DIVIDE(SUM(is_landing_page), COUNT(*)), 2) AS entrance_rate -- Entrance rate
    FROM 
        LandingPageData
    WHERE 
        landing_page IS NOT NULL -- Exclude null paths
    GROUP BY 
        landing_page
)

-- Retrieve the top 10 most popular landing pages
SELECT
    landing_page,
    unique_users,
    page_views,
    entrances,
    entrance_rate
FROM 
    AggregatedLandingPages
ORDER BY 
    unique_users DESC -- Rank by the number of unique users
LIMIT 10;
