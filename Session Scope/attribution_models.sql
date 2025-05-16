WITH
  events AS (
  SELECT
    TIMESTAMP_MICROS(event_timestamp) AS event_ts,
    CONCAT(user_pseudo_id,'-',event_name,'-',CAST(event_timestamp AS STRING)) AS event_id,
    user_pseudo_id AS user_pseudo_id,
    user_id,
    traffic_source.name AS utm_channel,
    traffic_source.medium AS utm_medium,
    traffic_source.source AS utm_source,
    event_name AS event_type,
    (
    SELECT
      value.int_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'ga_session_id') AS session_id,
    (
    SELECT
      value.int_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'ga_session_number') AS session_number,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'page_referrer') AS referrer_host,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'page_location') AS page_path,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      event_name = 'page_view'
      AND KEY = 'page_title') AS page_title,
    ecommerce.purchase_revenue AS order_value,
    ecommerce.transaction_id AS order_id,
    platform AS channel,
    device.category AS device_category,
    device.operating_system,
    device.language,
    device.is_limited_ad_tracking,
    NULL AS browser,
    NULL AS hostname,
    geo.continent,
    geo.country,
    geo.region,
    geo.city
  FROM
-- Replace with your own project and dataset ID
  `project.dataset.events_*`     ),
  id_stitching AS (
  SELECT
    DISTINCT user_pseudo_id AS user_pseudo_id,
    LAST_VALUE(user_id IGNORE NULLS) OVER (PARTITION BY user_pseudo_id ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS user_id,
    MIN(event_ts) OVER (PARTITION BY user_pseudo_id ) AS first_seen_at,
    MAX(event_ts) OVER (PARTITION BY user_pseudo_id ) AS last_seen_at
  FROM
    events),
  sessions AS (
  SELECT
    user_pseudo_id,
    TIMESTAMP_MICROS(event_timestamp) AS session_start_ts,
    CAST(LEAD(TIMESTAMP_MICROS(event_timestamp),1) OVER (PARTITION BY CONCAT(user_pseudo_id)
      ORDER BY
        event_timestamp) AS timestamp) AS session_end_ts,
    (
    SELECT
      value.int_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'ga_session_id') AS session_id,
    (
    SELECT
      value.int_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'ga_session_number') AS session_number,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'page_referrer') AS referrer_host,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'page_location') AS landing_page_path,
    (
    SELECT
      value.string_value
    FROM
      UNNEST(event_params)
    WHERE
      event_name = 'page_view'
      AND KEY = 'page_title') AS landing_page_title,
    traffic_source.name AS utm_campaign,
    traffic_source.medium AS utm_medium,
    traffic_source.source AS utm_source,
    platform AS channel,
    CASE
      WHEN device.category = "desktop" THEN "desktop"
      WHEN device.category = "tablet"
    AND app_info.id IS NULL THEN "tablet-web"
      WHEN device.category = "mobile" AND app_info.id IS NULL THEN "mobile-web"
      WHEN device.category = "tablet"
    AND app_info.id IS NOT NULL THEN "tablet-app"
      WHEN device.category = "mobile" AND app_info.id IS NOT NULL THEN "mobile-app"
  END
    AS device,
    device.mobile_brand_name mobile_brand_name,
    device.mobile_model_name mobile_model_name,
    device.mobile_marketing_name mobile_marketing_name,
    device.mobile_os_hardware_model mobile_os_hardware_model,
    device.operating_system operating_system,
    device.operating_system_version operating_system_version,
    device.vendor_id vendor_id,
    device.advertising_id advertising_id,
    device.language LANGUAGE,
    device.is_limited_ad_tracking is_limited_ad_tracking,
    device.time_zone_offset_seconds,
    NULL AS browser,
    NULL AS browser_version,
    NULL AS browser,
    device.web_info.browser_version,
    NULL AS hostname,
    geo.continent continent,
    geo.country country,
    geo.region region,
    geo.city city,
    COUNT(DISTINCT CONCAT(user_pseudo_id,'-',event_name,'-',CAST(event_timestamp AS STRING))) OVER (PARTITION BY (SELECT value.int_value FROM UNNEST(event_params)
      WHERE
        KEY = 'ga_session_id')) AS events
  FROM
    -- Replace with your own project and dataset ID
  `project.dataset.events_*`  s 
  WHERE
    event_name = 'session_start' ),
  user_stitched_sessions AS (
  SELECT
    sessions.*,
    COALESCE(id_stitching.user_id, sessions.user_pseudo_id) AS blended_user_id
  FROM
    sessions
  LEFT JOIN
    id_stitching
  USING
    (user_pseudo_id) ),
  user_stitched_events AS (
  SELECT
    events.*,
    COALESCE(id_stitching.user_id, events.user_pseudo_id) AS blended_user_id
  FROM
    events
  LEFT JOIN
    id_stitching
  USING
    (user_pseudo_id) ),
  events_filtered AS (
  SELECT
    *
  FROM (
    SELECT
      *,
      FIRST_VALUE(CASE
          WHEN event_type = 'add_payment_info' THEN event_id
      END
        IGNORE NULLS) OVER (PARTITION BY blended_user_id ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_registration_event_id,
      FIRST_VALUE(CASE
          WHEN event_type='purchase' THEN event_id
      END
        IGNORE NULLS) OVER (PARTITION BY blended_user_id ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS first_order_event_id
    FROM
      user_stitched_events )
  WHERE
    event_type = 'purchase'
    OR (event_type='add_payment_info'
      AND event_id = first_registration_event_id) ),
  converting_events AS (
  SELECT
    e.blended_user_id,
    session_id,
    event_type,
    order_id AS order_id,
    CASE
      WHEN event_type='purchase' AND event_id = first_order_event_id THEN order_value
    ELSE
    0
  END
    AS first_order_revenue,
    CASE
      WHEN event_type='purchase' AND event_id != first_order_event_id THEN order_value
    ELSE
    0
  END
    AS repeat_order_revenue,
    CASE
      WHEN event_type IN ('purchase' ) THEN 1
    ELSE
    0
  END
    AS count_conversions,
    CASE
      WHEN event_type='purchase' AND event_id = first_order_event_id THEN 1
    ELSE
    0
  END
    AS count_first_order_conversions,
    CASE
      WHEN event_type='purchase' AND event_id != first_order_event_id THEN 1
    ELSE
    0
  END
    AS count_repeat_order_conversions,
    CASE
      WHEN event_type = 'purchase' THEN 1
    ELSE
    0
  END
    AS count_order_conversions,
    CASE
      WHEN event_type='add_payment_info' AND event_id = first_registration_event_id THEN 1
    ELSE
    0
  END
    AS count_registration_conversions,
    event_ts AS converted_ts
  FROM
    events_filtered e ),
  converting_sessions_deduped AS (
  SELECT
    session_id AS session_id,
    MAX(blended_user_id) AS blended_user_id,
    SUM(first_order_revenue) AS first_order_revenue,
    SUM(repeat_order_revenue) AS repeat_order_revenue,
    SUM(count_first_order_conversions) AS count_first_order_conversions,
    SUM(count_repeat_order_conversions) AS count_repeat_order_conversions,
    SUM(count_order_conversions) AS count_order_conversions,
    SUM(count_registration_conversions) AS count_registration_conversions,
    SUM(count_registration_conversions) + SUM(count_first_order_conversions) + SUM(count_repeat_order_conversions) AS count_conversions,
    MAX(converted_ts) AS converted_ts,
    MIN(converted_ts) AS min_converted_ts
  FROM
    converting_events
  GROUP BY
    1 ),
  touchpoint_and_converting_sessions_labelled AS (
  SELECT
    *
  FROM (
    SELECT
      *,
      FIRST_VALUE(converted_ts IGNORE NULLS) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS conversion_cycle_conversion_ts,
      ROW_NUMBER() OVER (PARTITION BY blended_user_id ORDER BY session_start_ts) AS session_seq
    FROM (
      SELECT
        s.blended_user_id AS blended_user_id,
        s.session_id AS session_id,
        s.session_start_ts AS session_start_ts,
        s.session_end_ts AS session_end_ts,
        c.converted_ts AS converted_ts,
        c.min_converted_ts AS min_converted_ts,
        COALESCE(SUM(c.count_conversions),0) AS count_conversions,
        COALESCE(SUM(c.count_order_conversions),0) AS count_order_conversions,
        COALESCE(SUM(c.count_first_order_conversions),0) AS count_first_order_conversions,
        COALESCE(SUM(c.count_repeat_order_conversions),0) AS count_repeat_order_conversions,
        COALESCE(SUM(c.count_registration_conversions),0) AS count_registration_conversions,
        COALESCE(CASE
            WHEN c.count_conversions >0 THEN TRUE
          ELSE
          FALSE
        END
          ,FALSE) AS conversion_session,
        COALESCE(CASE
            WHEN c.count_conversions >0 THEN 1
          ELSE
          0
        END
          ,0) AS conversion_event,
        COALESCE(CASE
            WHEN c.count_order_conversions>0 THEN 1
          ELSE
          0
        END
          ,0) AS order_conversion_event,
        COALESCE(CASE
            WHEN c.count_registration_conversions>0 THEN 1
          ELSE
          0
        END
          ,0) AS registration_conversion_event,
            COALESCE(CASE
            WHEN c.count_first_order_conversions>0 THEN 1
          ELSE
          0
        END
          ,0) AS first_order_conversion_event,
        COALESCE(CASE
            WHEN c.count_repeat_order_conversions>0 THEN 1
          ELSE
          0
        END
          ,0) AS repeat_order_conversion_event,
        utm_source AS utm_source,
        CAST(NULL AS string) AS utm_content,
        utm_medium AS utm_medium,
        utm_campaign AS utm_campaign,
        referrer_host AS referrer_host,
        channel AS channel,
        CASE
          WHEN LOWER(utm_source) IN ('(direct)', '(data deleted)', '<other>') THEN FALSE
        ELSE
        TRUE
      END
        AS is_non_direct_channel,
        CASE
          WHEN LOWER(utm_medium) LIKE '%paid%' THEN TRUE
        ELSE
        FALSE
      END
        AS is_paid_channel,
        events AS events,
        c.first_order_revenue,
        c.repeat_order_revenue,
        city,
        continent,
        country,
        region
      FROM
        user_stitched_sessions s
      LEFT JOIN
        converting_sessions_deduped c
      ON
        s.session_id = c.session_id
      GROUP BY ALL) )
  WHERE
    conversion_cycle_conversion_ts >= session_start_ts ),
      
  touchpoint_and_converting_sessions_labelled_with_conversion_number AS (
  SELECT
    *,
    SUM(conversion_event) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS user_total_conversions,
    SUM(count_order_conversions) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS user_total_order_conversions,
    SUM(count_registration_conversions) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS user_total_registration_conversions,
    SUM(count_first_order_conversions) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS user_total_first_order_conversions,
    SUM(count_repeat_order_conversions) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS user_total_repeat_order_conversions
  FROM
    touchpoint_and_converting_sessions_labelled ),

  touchpoint_and_converting_sessions_labelled_with_conversion_number_and_conversion_cycles AS (
  SELECT
    *,
    CASE
      WHEN registration_conversion_event = 0 THEN MAX(COALESCE(user_total_registration_conversions,0)) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) + 1
    ELSE
    MAX(user_total_registration_conversions) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
  END
    AS user_registration_conversion_cycle,
    CASE
      WHEN conversion_event = 0 THEN MAX(COALESCE(user_total_conversions,0)) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) + 1
    ELSE
    MAX(user_total_conversions) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
  END
    AS user_conversion_cycle,
    CASE
      WHEN first_order_conversion_event = 0 THEN MAX(COALESCE(user_total_first_order_conversions,0)) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) + 1
    ELSE
    MAX(user_total_first_order_conversions) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
  END
    AS user_first_order_conversion_cycle,
    CASE
      WHEN repeat_order_conversion_event = 0 THEN MAX(COALESCE(user_total_repeat_order_conversions,0)) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) + 1
    ELSE
    MAX(user_total_repeat_order_conversions) OVER (PARTITION BY blended_user_id ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
  END
    AS user_repeat_order_conversion_cycle
  FROM
    touchpoint_and_converting_sessions_labelled_with_conversion_number ),
  touchpoint_and_converting_sessions_labelled_with_conversion_number_and_conversion_cycles_and_day_number AS (
  SELECT
    *,
    (DATE_DIFF(DATE(session_start_ts),DATE('2018-01-01'),DAY)) AS session_day_number
  FROM
    touchpoint_and_converting_sessions_labelled_with_conversion_number_and_conversion_cycles ),

  days_to_each_conversion AS (
    SELECT
    *,
    MAX(session_day_number) OVER (PARTITION BY blended_user_id, user_conversion_cycle) - session_day_number AS days_before_conversion,
    (MAX(session_day_number) OVER (PARTITION BY blended_user_id, user_conversion_cycle) - session_day_number )<= 30 AS is_within_attribution_lookback_window,
    (MAX(session_day_number) OVER (PARTITION BY blended_user_id, user_conversion_cycle) - session_day_number ) <= 7 AS is_within_attribution_time_decay_days_window
  FROM
    touchpoint_and_converting_sessions_labelled_with_conversion_number_and_conversion_cycles_and_day_number ),

  add_time_decay_score AS (
  SELECT
    *,
  IF
    (is_within_attribution_time_decay_days_window, POW(2,days_before_conversion-1)/NULLIF(7,0),NULL) AS time_decay_score,
  IF
    (conversion_session,1,POW(2, (days_before_conversion - 1))) AS weighting,
  IF
    (conversion_session,1,(COUNT(CASE
            WHEN NOT conversion_session OR TRUE THEN session_id
        END
          ) OVER (PARTITION BY blended_user_id, DATE_TRUNC(CAST(session_start_ts AS date),DAY)))) AS sessions_within_day_to_conversion,
  IF
    (conversion_session,1,safe_divide (POW(2, (days_before_conversion - 1)),
        COUNT(CASE
            WHEN NOT conversion_session OR TRUE THEN session_id
        END
          ) OVER (PARTITION BY blended_user_id, DATE_TRUNC(CAST(session_start_ts AS date),DAY)))) AS weighting_split_by_days_sessions
  FROM
    days_to_each_conversion ),

 
      split_time_decay_score_across_days_sessions AS (
  SELECT
    *,
    time_decay_score/NULLIF(sessions_within_day_to_conversion,0) AS apportioned_time_decay_score
  FROM
    add_time_decay_score ),

      
  attrib_calc_flags AS (
  SELECT
    *,
  IF
    (FIRST_VALUE(CASE
          WHEN is_within_attribution_lookback_window AND is_non_direct_channel = TRUE THEN session_id
      END
        IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) = session_id,TRUE,FALSE) AS is_first_non_direct_channel_in_conversion_cycle,
  IF
    (LAST_VALUE(CASE
          WHEN is_within_attribution_lookback_window AND is_non_direct_channel = TRUE THEN session_id
      END
        IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)=session_id,TRUE,FALSE) AS is_last_non_direct_channel_in_conversion_cycle,
  IF
    (SUM(CASE
          WHEN is_within_attribution_lookback_window AND is_non_direct_channel = TRUE THEN 1
      END
        ) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)>0,TRUE,FALSE) AS is_conversion_cycle_with_non_direct,
  IF
    (FIRST_VALUE(CASE
          WHEN is_within_attribution_lookback_window AND is_paid_channel = TRUE THEN session_id
      END
        IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)=session_id,TRUE,FALSE) AS is_first_paid_channel_in_conversion_cycle,
  IF
    (LAST_VALUE(CASE
          WHEN is_within_attribution_lookback_window AND is_paid_channel = TRUE THEN session_id
      END
        IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)=session_id,TRUE,FALSE) AS is_last_paid_channel_in_conversion_cycle,
  IF
    (SUM(CASE
          WHEN is_within_attribution_lookback_window AND is_paid_channel = TRUE THEN 1
      END
        ) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)>0,TRUE,FALSE) AS is_conversion_cycle_with_paid
  FROM
    split_time_decay_score_across_days_sessions ),
  session_attrib_pct AS (
  SELECT
    *,
  IF
    (conversion_session
      AND NOT TRUE,0,
      CASE
        WHEN session_id = LAST_VALUE( IF (is_within_attribution_lookback_window,session_id,NULL) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
      ELSE
      0
    END
      ) AS last_click_attrib_pct,
  IF
    (conversion_session
      AND NOT TRUE,0,
      CASE
        WHEN is_last_non_direct_channel_in_conversion_cycle THEN 1 
        WHEN
    IF
      (NOT is_conversion_cycle_with_non_direct
        AND session_id = LAST_VALUE(
        IF
          (is_within_attribution_lookback_window,session_id,NULL) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),TRUE,FALSE) = TRUE THEN 1 
      ELSE
      0 
    END
      ) AS last_non_direct_click_attrib_pct,
  IF
    (conversion_session
      AND NOT TRUE,0,
      CASE
        WHEN is_last_paid_channel_in_conversion_cycle THEN 1 
        WHEN
    IF
      (NOT is_conversion_cycle_with_paid
        AND session_id = LAST_VALUE(
        IF
          (is_within_attribution_lookback_window,session_id,NULL) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),TRUE,FALSE) = TRUE THEN 1
      ELSE
      0 
    END
      ) AS last_paid_click_attrib_pct,
  IF
    (conversion_session
      AND NOT TRUE,0,
      CASE
        WHEN session_id = FIRST_VALUE( IF (is_within_attribution_lookback_window,session_id,NULL) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
      ELSE
      0
    END
      ) AS first_click_attrib_pct,
  IF
    (conversion_session
      AND NOT TRUE,0,
      CASE
        WHEN is_first_non_direct_channel_in_conversion_cycle THEN 1 
        WHEN
    IF
      (NOT is_conversion_cycle_with_non_direct
        AND session_id = FIRST_VALUE(
        IF
          (is_within_attribution_lookback_window,session_id,NULL) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),TRUE,FALSE) = TRUE THEN 1
      ELSE
      0 
    END
      ) AS first_non_direct_click_attrib_pct,
  IF
    (conversion_session
      AND NOT TRUE,0,
      CASE
        WHEN is_first_paid_channel_in_conversion_cycle THEN 1 
        WHEN
    IF
      (NOT is_conversion_cycle_with_paid
        AND session_id = FIRST_VALUE(
        IF
          (is_within_attribution_lookback_window,session_id,NULL) IGNORE NULLS) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),TRUE,FALSE) = TRUE THEN 1 
      ELSE
      0 
    END
      ) AS first_paid_click_attrib_pct,
  IF
    (conversion_session
      AND NOT TRUE,0,
    IF
      (is_within_attribution_lookback_window,(safe_divide (1,
            (COUNT(
              IF
                (is_within_attribution_lookback_window,session_id,NULL)) OVER (PARTITION BY blended_user_id, user_conversion_cycle ORDER BY session_start_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) + 0))),0) ) AS even_click_attrib_pct,
  IF
    (conversion_session
      AND NOT TRUE,0,CASE
        WHEN is_within_attribution_time_decay_days_window THEN apportioned_time_decay_score / NULLIF((SUM(apportioned_time_decay_score) OVER (PARTITION BY blended_user_id, user_conversion_cycle)),0)
    END
      ) AS time_decay_attrib_pct
  FROM
    attrib_calc_flags ),
  
  final AS (
  SELECT
    'Last Click' AS MODEL,
    (MAX(count_registration_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_click_attrib_pct) AS user_registration_conversions,
    (MAX(count_first_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_click_attrib_pct) AS first_order_conversions,
    (MAX(count_repeat_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_click_attrib_pct) AS repeat_order_conversions,
    (MAX(first_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_click_attrib_pct) AS first_order_revenue,
    (MAX(repeat_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_click_attrib_pct) AS repeat_order_revenue,
    blended_user_id,
    session_id,
    session_start_ts,
    session_end_ts,
    session_seq,
    user_conversion_cycle,
    is_first_non_direct_channel_in_conversion_cycle,
    is_last_non_direct_channel_in_conversion_cycle,
    is_conversion_cycle_with_non_direct,
    is_first_paid_channel_in_conversion_cycle,
    is_last_paid_channel_in_conversion_cycle,
    is_conversion_cycle_with_paid,
    is_non_direct_channel,
    is_paid_channel,
    conversion_session AS is_conversion_session,
    utm_source,
    utm_medium,
    utm_campaign,
    referrer_host,
    channel,
    city,
    continent,
    country,
    region
  FROM
    session_attrib_pct a
  UNION ALL
  SELECT
    'First Click' AS MODEL,
    (MAX(count_registration_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* first_click_attrib_pct) AS user_registration_conversions,
    (MAX(count_first_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* first_click_attrib_pct) AS first_order_conversions,
    (MAX(count_repeat_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* first_click_attrib_pct) AS repeat_order_conversions,
    (MAX(first_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* first_click_attrib_pct) AS first_order_revenue,
    (MAX(repeat_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* first_click_attrib_pct) AS repeat_order_revenue,
    blended_user_id,
    session_id,
    session_start_ts,
    session_end_ts,
    session_seq,
    user_conversion_cycle,
    is_first_non_direct_channel_in_conversion_cycle,
    is_last_non_direct_channel_in_conversion_cycle,
    is_conversion_cycle_with_non_direct,
    is_first_paid_channel_in_conversion_cycle,
    is_last_paid_channel_in_conversion_cycle,
    is_conversion_cycle_with_paid,
    is_non_direct_channel,
    is_paid_channel,
    conversion_session AS is_conversion_session,
    utm_source,
    utm_medium,
    utm_campaign,
    referrer_host,
    channel,
    city,
    continent,
    country,
    region
  FROM
    session_attrib_pct a
  UNION ALL
  SELECT
    'Last Non-Direct Click' AS MODEL,
    (MAX(count_registration_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_non_direct_click_attrib_pct) AS user_registration_conversions,
    (MAX(count_first_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_non_direct_click_attrib_pct) AS first_order_conversions,
    (MAX(count_repeat_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_non_direct_click_attrib_pct) AS repeat_order_conversions,
    (MAX(first_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_non_direct_click_attrib_pct) AS first_order_revenue,
    (MAX(repeat_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_non_direct_click_attrib_pct) AS repeat_order_revenue,
    blended_user_id,
    session_id,
    session_start_ts,
    session_end_ts,
    session_seq,
    user_conversion_cycle,
    is_first_non_direct_channel_in_conversion_cycle,
    is_last_non_direct_channel_in_conversion_cycle,
    is_conversion_cycle_with_non_direct,
    is_first_paid_channel_in_conversion_cycle,
    is_last_paid_channel_in_conversion_cycle,
    is_conversion_cycle_with_paid,
    is_non_direct_channel,
    is_paid_channel,
    conversion_session AS is_conversion_session,
    utm_source,
    utm_medium,
    utm_campaign,
    referrer_host,
    channel,
    city,
    continent,
    country,
    region
  FROM
    session_attrib_pct a
  UNION ALL
  SELECT
    'First Paid Click' AS MODEL,
    (MAX(count_registration_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* first_paid_click_attrib_pct) AS user_registration_conversions,
    (MAX(count_first_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* first_paid_click_attrib_pct) AS first_order_conversions,
    (MAX(count_repeat_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* first_paid_click_attrib_pct) AS repeat_order_conversions,
    (MAX(first_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* first_paid_click_attrib_pct) AS first_order_revenue,
    (MAX(repeat_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* first_paid_click_attrib_pct) AS repeat_order_revenue,
    blended_user_id,
    session_id,
    session_start_ts,
    session_end_ts,
    session_seq,
    user_conversion_cycle,
    is_first_non_direct_channel_in_conversion_cycle,
    is_last_non_direct_channel_in_conversion_cycle,
    is_conversion_cycle_with_non_direct,
    is_first_paid_channel_in_conversion_cycle,
    is_last_paid_channel_in_conversion_cycle,
    is_conversion_cycle_with_paid,
    is_non_direct_channel,
    is_paid_channel,
    conversion_session AS is_conversion_session,
    utm_source,
    utm_medium,
    utm_campaign,
    referrer_host,
    channel,
    city,
    continent,
    country,
    region
  FROM
    session_attrib_pct a
  UNION ALL
  SELECT
    'Last Paid Click' AS MODEL,
    (MAX(count_registration_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_paid_click_attrib_pct) AS user_registration_conversions,
    (MAX(count_first_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_paid_click_attrib_pct) AS first_order_conversions,
    (MAX(count_repeat_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_paid_click_attrib_pct) AS repeat_order_conversions,
    (MAX(first_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_paid_click_attrib_pct) AS first_order_revenue,
    (MAX(repeat_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* last_paid_click_attrib_pct) AS repeat_order_revenue,
    blended_user_id,
    session_id,
    session_start_ts,
    session_end_ts,
    session_seq,
    user_conversion_cycle,
    is_first_non_direct_channel_in_conversion_cycle,
    is_last_non_direct_channel_in_conversion_cycle,
    is_conversion_cycle_with_non_direct,
    is_first_paid_channel_in_conversion_cycle,
    is_last_paid_channel_in_conversion_cycle,
    is_conversion_cycle_with_paid,
    is_non_direct_channel,
    is_paid_channel,
    conversion_session AS is_conversion_session,
    utm_source,
    utm_medium,
    utm_campaign,
    referrer_host,
    channel,
    city,
    continent,
    country,
    region
  FROM
    session_attrib_pct a
  UNION ALL
  SELECT
    'Linear' AS MODEL,
    (MAX(count_registration_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* even_click_attrib_pct) AS user_registration_conversions,
    (MAX(count_first_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* even_click_attrib_pct) AS first_order_conversions,
    (MAX(count_repeat_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* even_click_attrib_pct) AS repeat_order_conversions,
    (MAX(first_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* even_click_attrib_pct) AS first_order_revenue,
    (MAX(repeat_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* even_click_attrib_pct) AS repeat_order_revenue,
    blended_user_id,
    session_id,
    session_start_ts,
    session_end_ts,
    session_seq,
    user_conversion_cycle,
    is_first_non_direct_channel_in_conversion_cycle,
    is_last_non_direct_channel_in_conversion_cycle,
    is_conversion_cycle_with_non_direct,
    is_first_paid_channel_in_conversion_cycle,
    is_last_paid_channel_in_conversion_cycle,
    is_conversion_cycle_with_paid,
    is_non_direct_channel,
    is_paid_channel,
    conversion_session AS is_conversion_session,
    utm_source,
    utm_medium,
    utm_campaign,
    referrer_host,
    channel,
    city,
    continent,
    country,
    region
  FROM
    session_attrib_pct a
  UNION ALL
  SELECT
    'Time Decay' AS MODEL,
    (MAX(count_registration_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* time_decay_attrib_pct) AS user_registration_conversions,
    (MAX(count_first_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* time_decay_attrib_pct) AS first_order_conversions,
    (MAX(count_repeat_order_conversions) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* time_decay_attrib_pct) AS repeat_order_conversions,
    (MAX(first_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* time_decay_attrib_pct) AS first_order_revenue,
    (MAX(repeat_order_revenue) OVER (PARTITION BY blended_user_id, user_conversion_cycle)* time_decay_attrib_pct) AS repeat_order_revenue,
    blended_user_id,
    session_id,
    session_start_ts,
    session_end_ts,
    session_seq,
    user_conversion_cycle,
    is_first_non_direct_channel_in_conversion_cycle,
    is_last_non_direct_channel_in_conversion_cycle,
    is_conversion_cycle_with_non_direct,
    is_first_paid_channel_in_conversion_cycle,
    is_last_paid_channel_in_conversion_cycle,
    is_conversion_cycle_with_paid,
    is_non_direct_channel,
    is_paid_channel,
    conversion_session AS is_conversion_session,
    utm_source,
    utm_medium,
    utm_campaign,
    referrer_host,
    channel,
    city,
    continent,
    country,
    region
  FROM
    session_attrib_pct a ),
  pivoted AS (
  SELECT
    MODEL,
    blended_user_id,
    session_id,
    session_start_ts,
    session_end_ts,
    session_seq,
    user_conversion_cycle,
    is_first_non_direct_channel_in_conversion_cycle,
    is_last_non_direct_channel_in_conversion_cycle,
    is_conversion_cycle_with_non_direct,
    is_first_paid_channel_in_conversion_cycle,
    is_last_paid_channel_in_conversion_cycle,
    is_conversion_cycle_with_paid,
    is_non_direct_channel,
    is_paid_channel,
    is_conversion_session,
    utm_source,
    utm_medium,
    utm_campaign,
    referrer_host,
    channel,
    city,
    continent,
    country,
    region,
    user_registration_conversions,
    first_order_conversions,
    repeat_order_conversions,
    first_order_revenue,
    repeat_order_revenue
  FROM
    final)
SELECT
  *
FROM
  pivoted
