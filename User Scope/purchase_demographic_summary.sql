-- Declare date range variables to filter data for the year 2024
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    geo.country AS country,
    geo.city AS city,
    device.language AS language,
    device.web_info.browser AS browser,
    device.operating_system AS operating_system,
    device.category AS device_category,
    traffic_source.source AS traffic_source,
    traffic_source.medium AS traffic_medium,
    traffic_source.name AS campaign_name,
    item_details.item_category AS item_category,
    item_details.item_name AS item_name,
    COUNT(*) AS total_purchases,
    SUM(ecommerce.purchase_revenue) AS total_revenue,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(DISTINCT CASE WHEN (SELECT value.int_value 
                              FROM UNNEST(event_params) 
                              WHERE key = 'ga_session_number') = 1 
                        THEN user_pseudo_id 
                   END) AS new_users
-- Replace with your own project and dataset ID
FROM `project.dataset.events_*`,
UNNEST(items) AS item_details
WHERE event_name = 'purchase'
  AND _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '')
GROUP BY event_date, country, city, language, browser, operating_system, device_category, 
         traffic_source, traffic_medium, campaign_name, item_category, item_name
ORDER BY total_revenue DESC;
