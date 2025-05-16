-- Declare date range variables for filtering
DECLARE start_date STRING DEFAULT '2024-01-01';
DECLARE end_date STRING DEFAULT '2024-12-31';

SELECT
  -- Classify traffic sources into predefined channel groupings
  CASE
    WHEN traffic_source.source = "(direct)" 
         AND traffic_source.medium IN ("(not set)", "(none)") THEN "Direct"

    WHEN REGEXP_CONTAINS(traffic_source.source, r"alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart") 
         OR (REGEXP_CONTAINS(traffic_source.name, r"^(.*(([^a-df-z]|^)shop|shopping).*)$") 
         AND REGEXP_CONTAINS(traffic_source.medium, r"^(.*cp.*|ppc|paid.*)$")) THEN "Paid Shopping"

    WHEN REGEXP_CONTAINS(traffic_source.source, r"baidu|bing|duckduckgo|ecosia|google|yahoo|yandex") 
         AND REGEXP_CONTAINS(traffic_source.medium, r"^(.*cp.*|ppc|paid.*)$") THEN "Paid Search"

    WHEN REGEXP_CONTAINS(traffic_source.source, r"badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp") 
         AND REGEXP_CONTAINS(traffic_source.medium, r"^(.*cp.*|ppc|paid.*)$") THEN "Paid Social"

    WHEN REGEXP_CONTAINS(traffic_source.source, r"dailymotion|disneyplus|netflix|youtube|vimeo|twitch") 
         AND REGEXP_CONTAINS(traffic_source.medium, r"^(.*cp.*|ppc|paid.*)$") THEN "Paid Video"

    WHEN traffic_source.medium IN ("display", "banner", "expandable", "interstitial", "cpm") THEN "Display"

    WHEN REGEXP_CONTAINS(traffic_source.source, r"alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart") 
         OR REGEXP_CONTAINS(traffic_source.name, r"^(.*(([^a-df-z]|^)shop|shopping).*)$") THEN "Organic Shopping"

    WHEN REGEXP_CONTAINS(traffic_source.source, r"badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp") 
         OR traffic_source.medium IN ("social", "social-network", "social-media", "sm", "social network", "social media") THEN "Organic Social"

    WHEN REGEXP_CONTAINS(traffic_source.source, r"dailymotion|disneyplus|netflix|youtube|vimeo|twitch") 
         OR REGEXP_CONTAINS(traffic_source.medium, r"^(.*video.*)$") THEN "Organic Video"

    WHEN REGEXP_CONTAINS(traffic_source.source, r"baidu|bing|duckduckgo|ecosia|google|yahoo|yandex") 
         OR traffic_source.medium = "organic" THEN "Organic Search"

    WHEN REGEXP_CONTAINS(traffic_source.source, r"email|e-mail|e_mail|e mail") 
         OR REGEXP_CONTAINS(traffic_source.medium, r"email|e-mail|e_mail|e mail") THEN "Email"

    WHEN traffic_source.medium = "affiliate" THEN "Affiliates"
    WHEN traffic_source.medium = "referral" THEN "Referral"
    WHEN traffic_source.medium = "audio" THEN "Audio"
    WHEN traffic_source.medium = "sms" THEN "SMS"
    WHEN traffic_source.medium LIKE "%push" 
         OR REGEXP_CONTAINS(traffic_source.medium, r"mobile|notification") THEN "Mobile Push Notifications"

    ELSE "Unassigned"
  END AS channel_grouping,

  -- Traffic source details
  traffic_source.source AS source,
  traffic_source.medium AS medium,
  traffic_source.name AS campaign,

  -- Count distinct users
  COUNT(DISTINCT user_pseudo_id) AS users

FROM
  `project.dataset.events_*` -- Replace with your own project and dataset ID
WHERE
  _TABLE_SUFFIX BETWEEN REPLACE(start_date, '-', '') AND REPLACE(end_date, '-', '') -- Dynamic date filtering

GROUP BY
  channel_grouping,
  source,
  medium,
  campaign

ORDER BY
  users DESC; -- Order by user count for better insights
