 -- Declare date range variables for dynamic filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

WITH traffic_data AS ( 
  -- Extract session-level attributes
  SELECT
    CONCAT(user_pseudo_id, '-', 
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id,
    collected_traffic_source.manual_source AS session_source,
    collected_traffic_source.manual_medium AS session_medium,
    collected_traffic_source.manual_campaign_name AS session_campaign_name,
    collected_traffic_source.gclid
  FROM
    `project.dataset.events_*` -- Replace with your own project and dataset ID
  WHERE
    _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering
),

channel_grouping AS (
  -- Assign channel groupings based on session attributes
  SELECT
    session_id,
    CASE
      WHEN session_source IS NULL THEN "Direct"
      WHEN REGEXP_CONTAINS(session_campaign_name, "cross-network") THEN "Cross-network"
      WHEN (REGEXP_CONTAINS(session_source, r"alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart") 
            OR REGEXP_CONTAINS(session_campaign_name, r"^(.*(([^a-df-z]|^)shop|shopping).*)$")) 
           AND REGEXP_CONTAINS(session_medium, r"^(.*cp.*|ppc|paid.*)$") THEN "Paid Shopping"
      WHEN gclid IS NOT NULL 
           OR (REGEXP_CONTAINS(session_source, r"baidu|bing|duckduckgo|ecosia|google|yahoo|yandex") 
           AND REGEXP_CONTAINS(session_medium, r"^(.*cp.*|ppc|paid.*)$")) THEN "Paid Search"
      WHEN REGEXP_CONTAINS(session_source, r"badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp") 
           AND REGEXP_CONTAINS(session_medium, r"^(.*cp.*|ppc|paid.*)$") THEN "Paid Social"
      WHEN REGEXP_CONTAINS(session_source, r"dailymotion|disneyplus|netflix|youtube|vimeo|twitch") 
           AND REGEXP_CONTAINS(session_medium, r"^(.*cp.*|ppc|paid.*)$") THEN "Paid Video"
      WHEN session_medium IN ("display", "banner", "expandable", "interstitial", "cpm") THEN "Display"
      WHEN REGEXP_CONTAINS(session_source, r"alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart") 
           OR REGEXP_CONTAINS(session_campaign_name, r"^(.*(([^a-df-z]|^)shop|shopping).*)$") THEN "Organic Shopping"
      WHEN REGEXP_CONTAINS(session_source, r"badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp") 
           OR session_medium IN ("social", "social-network", "social-media", "sm", "social network", "social media") THEN "Organic Social"
      WHEN REGEXP_CONTAINS(session_source, r"dailymotion|disneyplus|netflix|youtube|vimeo|twitch") 
           OR REGEXP_CONTAINS(session_medium, r"^(.*video.*)$") THEN "Organic Video"
      WHEN REGEXP_CONTAINS(session_source, r"baidu|bing|duckduckgo|ecosia|google|yahoo|yandex") 
           OR session_medium = "organic" THEN "Organic Search"
      WHEN REGEXP_CONTAINS(session_source, r"email|e-mail|e_mail|e mail") 
           OR REGEXP_CONTAINS(session_medium, r"email|e-mail|e_mail|e mail") THEN "Email"
      WHEN session_medium = "affiliate" THEN "Affiliates"
      WHEN session_medium = "referral" THEN "Referral"
      WHEN session_medium = "audio" THEN "Audio"
      WHEN session_medium = "sms" THEN "SMS"
      WHEN session_medium LIKE "%push" 
           OR REGEXP_CONTAINS(session_medium, r"mobile|notification") THEN "Mobile Push Notifications"
      ELSE "Unassigned"
    END AS channel_grouping
  FROM
    traffic_data
)

SELECT
  channel_grouping,
  COUNT(DISTINCT session_id) AS session_count -- Count unique sessions per channel grouping
FROM
  channel_grouping
GROUP BY
  channel_grouping
ORDER BY
  session_count DESC; 
