
  WITH price_percentile_split AS (
  SELECT
    bid__app_platform AS platform
    , bid__bid_request__exchange AS exchange
    , bid__price_data__model_type AS model_type
    , CASE WHEN bid__creative__ad_format = 'video' THEN 'VAST'
           WHEN bid__creative__ad_format = 'native' THEN 'native'
           WHEN bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
           WHEN  bid__creative__ad_format = '300x250' THEN  'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , approx_percentile(bid__price_data__conversion_likelihood,
        ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.9999]) AS price_percentile
  FROM rtb.impressions_with_bids
  WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1)}}'
    AND CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') > '2023-03-01'
    AND bid__price_data__model_type != ''
  GROUP BY 1,2,3,4
  )
  , percentile_bucket AS (
  SELECT
  platform
  , exchange
  , ad_format
  , model_type
  , CAST(
        zip(
         ARRAY[0] || price_percentile, price_percentile || CAST(ARRAY[null] as ARRAY(DOUBLE)),
         ARRAY['0-10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', '70-80', '80-90', '90-95', '95-99', '99-99.99', '99.99-100']
        ) AS ARRAY(ROW(low DOUBLE, high DOUBLE, name VARCHAR))) AS percentiles
FROM price_percentile_split
 )
, convx_buckets AS (
  SELECT 
  platform
  , model_type
  , exchange
  , ad_format
  , p.low
  , p.high
  , p.name
  FROM percentile_bucket
  CROSS JOIN UNNEST(percentiles) AS p
)
, latest_sfdc_partition AS (
    SELECT MAX(dt) AS latest_dt 
    FROM salesforce_daily.customer_campaign__c  
    WHERE from_iso8601_timestamp(dt) >= CURRENT_TIMESTAMP - INTERVAL '2' DAY
)
 , saleforce_data AS (
    SELECT 
      b.id AS campaign_id
      , sd.sales_region__c as sales_region
      , sd.service_level__c AS service_level
      , sd.sales_sub_region__c AS sales_sub_region
    FROM salesforce_daily.customer_campaign__c sd 
    JOIN pinpoint.public.campaigns b      
        ON sd.campaign_id_18_digit__c = b.salesforce_campaign_id
    WHERE sd.dt = (select latest_dt FROM latest_sfdc_partition)
)
, goals AS (
   SELECT
    campaign_id,
    ARBITRARY(IF(priority = 1, type, NULL)) AS goal_1,
    ARBITRARY(IF(priority = 2, type, NULL)) AS goal_2,
    ARBITRARY(IF(priority = 3, type, NULL)) AS goal_3,
    ARBITRARY(IF(priority = 1, target_value, NULL)) AS goal_1_value,
    ARBITRARY(IF(priority = 2, target_value, NULL)) AS goal_2_value,
    ARBITRARY(IF(priority = 3, target_value, NULL)) AS goal_3_value
   FROM pinpoint.public.goals
   GROUP BY 1
)
, targets AS (
   SELECT 
    campaign_id
    , target AS treasurer_target
   FROM pinpoint.public.campaign_treasurer_configs
)
, funnel AS (
    -- fetch impressions
    SELECT
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS impression_at
    , NULL AS click_at
    , NULL AS install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
    , bid__app_platform AS platform
    , bid__bid_request__exchange AS exchange
    , bid__bid_request__device__geo__country AS country
    , bid__customer_id AS customer_id
    , bid__app_id AS dest_app_id
    , bid__campaign_id AS campaign_id
    , bid__ad_group_id AS ad_group_id
    , bid__ad_group_type AS ad_group_type
    , bid__creative__type AS creative_type
    , CASE WHEN bid__creative__ad_format = 'video' THEN 'VAST'
           WHEN bid__creative__ad_format = 'native' THEN 'native'
           WHEN bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
           WHEN  bid__creative__ad_format = '300x250' THEN  'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , 'N/A' AS  is_viewthrough
    , bid__price_data__model_type AS model_type
    , bid__campaign_tracker_type AS campaign_tracker_type
    , cb.name AS convx_percentile
    , cb.low AS convx_percentile_low
    , cb.high AS convx_percentile_high
    , NULL AS click_source
    , sum(1) AS impressions
    , sum(0) AS clicks
    , sum(0) AS installs
    , sum(spend_micros) AS internal_spend_micros
    , sum(revenue_micros) AS external_spend_micros
    , sum(0) AS customer_revenue_micros_d7
    , sum(0) AS target_events_d7
    , sum(0) AS target_events_first_d7
    , sum(bid__price_data__conversion_likelihood) AS predicted_conversion_likelihood
    , SUM(bid__price_data__predicted_imp_to_click_rate) AS predicted_clicks
    , SUM(COALESCE(bid__price_data__predicted_imp_to_click_rate * bid__price_data__predicted_click_to_install_rate, 0)
        + COALESCE(bid__price_data__predicted_imp_to_install_ct_rate, 0)) as predicted_installs_ct
    , SUM(bid__price_data__predicted_imp_to_install_vt_rate) AS predicted_installs_vt
    , SUM((COALESCE(bid__price_data__predicted_imp_to_click_rate * bid__price_data__predicted_click_to_install_rate, 0)
        + COALESCE(bid__price_data__predicted_imp_to_install_ct_rate, 0)) * bid__price_data__predicted_install_to_preferred_app_event_rate) AS predicted_target_events_ct
    , SUM(bid__price_data__predicted_imp_to_install_vt_rate * bid__price_data__predicted_install_to_preferred_app_event_vt_rate) AS predicted_target_events_vt
    , SUM((COALESCE(bid__price_data__predicted_imp_to_click_rate * bid__price_data__predicted_click_to_install_rate, 0)
        + COALESCE(bid__price_data__predicted_imp_to_install_ct_rate, 0)) * LEAST(bid__price_data__predicted_install_to_revenue_rate,500000000)) AS predicted_customer_revenue_micros_ct
    , SUM(bid__price_data__predicted_imp_to_install_vt_rate * LEAST(bid__price_data__predicted_install_to_revenue_rate,500000000)) AS predicted_customer_revenue_micros_vt
    FROM rtb.impressions_with_bids a
    JOIN convx_buckets cb
        ON a.bid__app_platform = cb.platform
            AND a.bid__price_data__model_type = cb.model_type
            AND (CASE WHEN bid__creative__ad_format = 'video' THEN 'VAST'
                      WHEN bid__creative__ad_format = 'native' THEN 'native'
                      WHEN bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
                      WHEN bid__creative__ad_format = '300x250' THEN  'mrec'
                      ELSE 'html-interstitial' END) = cb.ad_format
            AND a.bid__bid_request__exchange = cb.exchange
            AND a.bid__price_data__conversion_likelihood >= cb.low 
            AND (cb.high IS NULL OR a.bid__price_data__conversion_likelihood < cb.high)
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

    UNION ALL 
    -- fetch ad clicks
    SELECT
    CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(impression__at/1000, 'UTC'))),1,19),'Z') AS impression_at
    , CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS click_at
    , NULL AS install_at
    , CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
    , impression__bid__app_platform as platform
    , impression__bid__bid_request__exchange as exchange
    , geo__country AS country
    , impression__bid__customer_id as customer_id
    , impression__bid__app_id as dest_app_id
    , impression__bid__campaign_id as campaign_id
    , impression__bid__ad_group_id AS ad_group_id
    , impression__bid__ad_group_type AS ad_group_type
    , impression__bid__creative__type AS creative_type
    , CASE WHEN impression__bid__creative__ad_format = 'video' THEN 'VAST'
           WHEN impression__bid__creative__ad_format = 'native' THEN 'native'
           WHEN impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
           WHEN  impression__bid__creative__ad_format = '300x250' THEN 'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , 'N/A' AS is_viewthrough
    , impression__bid__price_data__model_type AS model_type
    , impression__bid__campaign_tracker_type AS campaign_tracker_type
    , cb.name AS convx_percentile
    , cb.low AS convx_percentile_low
    , cb.high AS convx_percentile_high
    , click_source AS click_source
    , sum(0) AS impressions
    , sum(1) AS clicks
    , sum(0) AS installs
    , sum(0) AS internal_spend_micros
    , sum(0) AS external_spend_micros
    , sum(0) AS customer_revenue_micros_d7
    , sum(0) AS target_events_d7
    , sum(0) AS target_events_first_d7
    , sum(0) AS predicted_conversion_likelihood
    , sum(0) AS predicted_clicks
    , sum(0) AS predicted_installs_ct
    , sum(0) AS predicted_installs_vt
    , sum(0) AS predicted_target_events_ct
    , sum(0) AS predicted_target_events_vt
    , sum(0) AS predicted_customer_revenue_micros_ct
    , sum(0) AS predicted_customer_revenue_micros_vt
    FROM rtb.ad_clicks a
    JOIN convx_buckets cb
        ON a.impression__bid__app_platform = cb.platform
            AND a.impression__bid__price_data__model_type = cb.model_type
            AND (CASE WHEN impression__bid__creative__ad_format = 'video' THEN 'VAST'
                      WHEN impression__bid__creative__ad_format = 'native' THEN 'native'
                      WHEN impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
                      WHEN impression__bid__creative__ad_format = '300x250' THEN 'mrec'
                      ELSE 'html-interstitial' END) = cb.ad_format
            AND a.impression__bid__bid_request__exchange = cb.exchange
            AND a.impression__bid__price_data__conversion_likelihood >= cb.low 
            AND (cb.high IS NULL OR a.impression__bid__price_data__conversion_likelihood < cb.high)
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
        AND has_prior_click = FALSE
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
    
    UNION ALL 
     -- fetch view clicks
    SELECT
    CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(impression__at/1000, 'UTC'))),1,19),'Z') AS impression_at
    , CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS click_at
    , NULL AS install_at
    , CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
    , impression__bid__app_platform as platform
    , impression__bid__bid_request__exchange as exchange
    , geo__country AS country
    , impression__bid__customer_id as customer_id
    , impression__bid__app_id as dest_app_id
    , impression__bid__campaign_id as campaign_id
    , impression__bid__ad_group_id AS ad_group_id
    , impression__bid__ad_group_type AS ad_group_type
    , impression__bid__creative__type AS creative_type
    , CASE WHEN impression__bid__creative__ad_format = 'video' THEN 'VAST'
           WHEN impression__bid__creative__ad_format = 'native' THEN 'native'
           WHEN impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
           WHEN  impression__bid__creative__ad_format = '300x250' THEN 'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , 'N/A' AS is_viewthrough
    , impression__bid__price_data__model_type AS model_type
    , impression__bid__campaign_tracker_type AS campaign_tracker_type
    , cb.name AS convx_percentile
    , cb.low AS convx_percentile_low
    , cb.high AS convx_percentile_high
    , click_source AS click_source
    , sum(0) AS impressions
    , sum(1) AS clicks
    , sum(0) AS installs
    , sum(0) AS internal_spend_micros
    , sum(0) AS external_spend_micros
    , sum(0) AS customer_revenue_micros_d7
    , sum(0) AS target_events_d7
    , sum(0) AS target_events_first_d7
    , sum(0) AS predicted_conversion_likelihood
    , sum(0) AS predicted_clicks
    , sum(0) AS predicted_installs_ct
    , sum(0) AS predicted_installs_vt
    , sum(0) AS predicted_target_events_ct
    , sum(0) AS predicted_target_events_vt
    , sum(0) AS predicted_customer_revenue_micros_ct
    , sum(0) AS predicted_customer_revenue_micros_vt
    FROM rtb.view_clicks a
    JOIN convx_buckets cb
        ON a.impression__bid__app_platform = cb.platform
            AND a.impression__bid__price_data__model_type = cb.model_type
            AND (CASE WHEN impression__bid__creative__ad_format = 'video' THEN 'VAST'
                      WHEN impression__bid__creative__ad_format = 'native' THEN 'native'
                      WHEN impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
                      WHEN impression__bid__creative__ad_format = '300x250' THEN 'mrec'
                      ELSE 'html-interstitial' END) = cb.ad_format
            AND a.impression__bid__bid_request__exchange = cb.exchange
            AND a.impression__bid__price_data__conversion_likelihood >= cb.low 
            AND (cb.high IS NULL OR a.impression__bid__price_data__conversion_likelihood < cb.high)
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
        AND has_prior_click = FALSE
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
    
    UNION ALL    
    -- fetch installs
    SELECT
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__impression__at/1000, 'UTC'))),1,19),'Z') as impression_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__at/1000, 'UTC'))),1,19),'Z') AS click_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') as install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
    , ad_click__impression__bid__app_platform AS platform
    , ad_click__impression__bid__bid_request__exchange AS exchange
    , geo__country AS country
    , ad_click__impression__bid__customer_id AS customer_id
    , ad_click__impression__bid__app_id AS dest_app_id
    , ad_click__impression__bid__campaign_id AS campaign_id
    , ad_click__impression__bid__ad_group_id AS ad_group_id
    , ad_click__impression__bid__ad_group_type AS ad_group_type
    , ad_click__impression__bid__creative__type AS creative_type
    , CASE WHEN ad_click__impression__bid__creative__ad_format = 'video' THEN 'VAST'
           WHEN ad_click__impression__bid__creative__ad_format = 'native' THEN 'native'
           WHEN ad_click__impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
           WHEN ad_click__impression__bid__creative__ad_format = '300x250' THEN 'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , CAST(is_viewthrough AS VARCHAR) AS is_viewthrough
    , ad_click__impression__bid__price_data__model_type AS model_type
    , ad_click__impression__bid__campaign_tracker_type as campaign_tracker_type
    , cb.name AS convx_percentile
    , cb.low AS convx_percentile_low
    , cb.high AS convx_percentile_high
    , ad_click__click_source AS click_source
    , sum(0) AS impressions
    , sum(0) AS clicks
    , sum(1) AS installs
    , sum(0) AS internal_spend_micros
    , sum(0) AS external_spend_micros
    , sum(0) AS customer_revenue_micros_d7
    , sum(0) AS target_events_d7
    , sum(0) AS target_events_first_d7
    , sum(0) AS predicted_conversion_likelihood
    , sum(0) AS predicted_clicks
    , sum(0) AS predicted_installs_ct
    , sum(0) AS predicted_installs_vt
    , sum(0) AS predicted_target_events_ct
    , sum(0) AS predicted_target_events_vt
    , sum(0) AS predicted_customer_revenue_micros_ct
    , sum(0) AS predicted_customer_revenue_micros_vt
    FROM rtb.matched_installs a
    JOIN convx_buckets cb
        ON a.ad_click__impression__bid__app_platform = cb.platform
            AND a.ad_click__impression__bid__price_data__model_type = cb.model_type
            AND (CASE WHEN ad_click__impression__bid__creative__ad_format = 'video' THEN 'VAST'
                      WHEN ad_click__impression__bid__creative__ad_format = 'native' THEN 'native'
                      WHEN ad_click__impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
                      WHEN ad_click__impression__bid__creative__ad_format = '300x250' THEN 'mrec'
                      ELSE 'html-interstitial' END) = cb.ad_format
            AND a.ad_click__impression__bid__bid_request__exchange = cb.exchange
            AND a.ad_click__impression__bid__price_data__conversion_likelihood >= cb.low 
            AND (cb.high IS NULL OR a.ad_click__impression__bid__price_data__conversion_likelihood < cb.high)
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

    UNION ALL 
    -- to fetch down funnel data (we are using 7d cohorted by installs data)

    SELECT
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(COALESCE(attribution_event__click__impression__at, reeng_click__impression__at, install__ad_click__impression__at)/1000, 'UTC'))),1,19),'Z') AS impression_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(COALESCE(attribution_event__click__at, reeng_click__at, install__ad_click__at)/1000, 'UTC'))),1,19),'Z') AS click_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(install__at/1000, 'UTC'))),1,19),'Z') AS install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
    , COALESCE(attribution_event__click__impression__bid__app_platform, reeng_click__impression__bid__app_platform, install__ad_click__impression__bid__app_platform) AS platform
    , COALESCE(attribution_event__click__impression__bid__bid_request__exchange, reeng_click__impression__bid__bid_request__exchange, install__ad_click__impression__bid__bid_request__exchange) AS exchange
    , COALESCE(attribution_event__click__geo__country, reeng_click__geo__country, install__geo__country) AS country
    , COALESCE(attribution_event__click__impression__bid__customer_id, reeng_click__impression__bid__customer_id, install__ad_click__impression__bid__customer_id) AS customer_id
    , COALESCE(attribution_event__click__impression__bid__app_id, reeng_click__impression__bid__app_id, install__ad_click__impression__bid__app_id) AS dest_app_id 
    , COALESCE(attribution_event__click__impression__bid__campaign_id, reeng_click__impression__bid__campaign_id, install__ad_click__impression__bid__campaign_id) AS campaign_id
    , COALESCE(attribution_event__click__impression__bid__ad_group_id, reeng_click__impression__bid__ad_group_id, install__ad_click__impression__bid__ad_group_id) AS ad_group_id
    , COALESCE(attribution_event__click__impression__bid__ad_group_type, reeng_click__impression__bid__ad_group_type, install__ad_click__impression__bid__ad_group_type) AS ad_group_type
    , COALESCE(attribution_event__click__impression__bid__creative__type, reeng_click__impression__bid__creative__type, install__ad_click__impression__bid__creative__type) AS creative_type
    , CASE WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'video' THEN 'VAST'
           WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'native' THEN 'native'
           WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) IN ('320x50', '728x90') THEN 'banner'
           WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = '300x250' THEN  'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , CAST(is_viewthrough AS VARCHAR) AS is_viewthrough
    , COALESCE(attribution_event__click__impression__bid__price_data__model_type, reeng_click__impression__bid__price_data__model_type, install__ad_click__impression__bid__price_data__model_type) AS model_type
    , COALESCE(install__ad_click__impression__bid__campaign_tracker_type, reeng_click__impression__bid__campaign_tracker_type, attribution_event__click__impression__bid__campaign_tracker_type) AS campaign_tracker_type
    , cb.name AS convx_percentile
    , cb.low AS convx_percentile_low
    , cb.high AS convx_percentile_high    
    , COALESCE(attribution_event__click__click_source, reeng_click__click_source, install__ad_click__click_source) AS click_source
    , sum(0) AS impressions
    , sum(0) AS clicks
    , sum(0) AS installs
    , sum(0) AS internal_spend_micros
    , sum(0) AS external_spend_micros
    , sum(IF(customer_revenue_micros > -100000000000 AND customer_revenue_micros < 100000000000, customer_revenue_micros, 0)) AS customer_revenue_micros_d7
    , sum(IF(custom_event_id = COALESCE(install__ad_click__impression__bid__campaign_target_event_id, reeng_click__impression__bid__campaign_target_event_id),1,0)) AS target_events_d7
    , sum(IF(custom_event_id = COALESCE(install__ad_click__impression__bid__campaign_target_event_id,reeng_click__impression__bid__campaign_target_event_id) AND first_occurrence,1,0)) AS target_events_first_d7
    , sum(0) AS predicted_conversion_likelihood
    , sum(0) AS predicted_clicks
    , sum(0) AS predicted_installs_ct
    , sum(0) AS predicted_installs_vt
    , sum(0) AS predicted_target_events_ct
    , sum(0) AS predicted_target_events_vt
    , sum(0) AS predicted_customer_revenue_micros_ct
    , sum(0) AS predicted_customer_revenue_micros_vt
    FROM rtb.matched_app_events a
    JOIN convx_buckets cb
        ON COALESCE(install__ad_click__impression__bid__app_platform, reeng_click__impression__bid__app_platform, attribution_event__click__impression__bid__app_platform) = cb.platform
            AND COALESCE(install__ad_click__impression__bid__price_data__model_type, reeng_click__impression__bid__price_data__model_type, attribution_event__click__impression__bid__price_data__model_type) = cb.model_type
            AND (CASE WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'video' THEN 'VAST'
                      WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'native' THEN 'native'
                      WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) IN ('320x50', '728x90') THEN 'banner'
                      WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = '300x250' THEN  'mrec'
                      ELSE 'html-interstitial' END) = cb.ad_format
            AND COALESCE(install__ad_click__impression__bid__bid_request__exchange, reeng_click__impression__bid__bid_request__exchange, attribution_event__click__impression__bid__bid_request__exchange) = cb.exchange
            AND COALESCE(install__ad_click__impression__bid__price_data__conversion_likelihood, reeng_click__impression__bid__price_data__conversion_likelihood, attribution_event__click__impression__bid__price_data__conversion_likelihood) >= cb.low 
            AND (cb.high IS NULL OR COALESCE(install__ad_click__impression__bid__price_data__conversion_likelihood, reeng_click__impression__bid__price_data__conversion_likelihood, attribution_event__click__impression__bid__price_data__conversion_likelihood) < cb.high)
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
)
    SELECT
    f.impression_at
    , f.click_at
    , f.install_at
    , f.at
    , f.platform
    , f.exchange
    , f.country
    , f.customer_id
    , cu.company AS customer_name
    , f.dest_app_id
    , f.campaign_id
    , f.ad_group_id
    , f.ad_group_type
    , c.current_optimization_state
    , f.creative_type
    , f.ad_format
    , f.is_viewthrough 
    , c.display_name AS campaign_name
    , apps.display_name AS dest_app_name
    , IF(c.vt_cap > 0, TRUE, FALSE) AS is_enrolled_in_vio
    , sd.sales_region AS campaign_sales_region
    , sd.sales_sub_region AS campaign_sales_sub_region
    , ag.display_name as ad_group_name
    , ag.bid_type as ad_group_bid_type
    , ag.exploratory as exploratory_ad_group
    , f.model_type
    , CASE WHEN f.creative_type = 'VAST' AND apps.video_viewthrough_enabled = TRUE THEN TRUE
        WHEN f.ad_format in ('banner','mrec') AND f.creative_type = 'HTML' AND apps.banner_viewthrough_enabled = TRUE THEN TRUE
        WHEN f.ad_format = 'interstitial' AND f.creative_type = 'HTML' AND apps.interstitial_viewthrough_enabled = TRUE THEN TRUE
        WHEN f.ad_format = 'native' AND apps.native_viewthrough_enabled = TRUE THEN TRUE  
        ELSE FALSE END AS viewthrough_enabled
    , sd.service_level
    , f.campaign_tracker_type
    , f.convx_percentile
    , f.convx_percentile_low
    , f.convx_percentile_high
    , f.click_source
    , IF(f.dest_app_id IS NULL,'N/A',goals.goal_1) AS goal_type_1
    , IF(f.dest_app_id IS NULL, NULL,goals.goal_1_value) AS goal_1_value
    , IF(f.dest_app_id IS NULL,'N/A',goals.goal_2) AS goal_type_2
    , IF(f.dest_app_id IS NULL, NULL,goals.goal_2_value) AS goal_2_value
    , targets.treasurer_target AS treasurer_target 
    , sum(f.impressions) AS impressions
    , sum(f.clicks) AS clicks
    , sum(f.installs) AS installs
    , sum(f.internal_spend_micros) AS internal_spend_micros
    , sum(f.external_spend_micros) AS external_spend_micros
    , sum(IF(from_iso8601_timestamp(f.at) - from_iso8601_timestamp(IF(f.ad_group_type='reengagement', f.click_at, f.install_at)) < INTERVAL '7' DAY, f.customer_revenue_micros_d7, 0)) AS customer_revenue_micros_d7
    , sum(IF(from_iso8601_timestamp(f.at) - from_iso8601_timestamp(IF(f.ad_group_type='reengagement', f.click_at, f.install_at)) < INTERVAL '7' DAY, f.target_events_d7, 0)) AS target_events_d7
    , sum(IF(from_iso8601_timestamp(f.at) - from_iso8601_timestamp(IF(f.ad_group_type='reengagement', f.click_at, f.install_at)) < INTERVAL '7' DAY, f.target_events_first_d7, 0)) AS target_events_first_d7 
    , sum(predicted_conversion_likelihood) AS predicted_conversion_likelihood
    , sum(predicted_clicks) AS predicted_clicks
    , sum(predicted_installs_ct) AS predicted_installs_ct
    , sum(predicted_installs_vt) AS predicted_installs_vt
    , sum(predicted_target_events_ct) AS predicted_target_events_ct
    , sum(predicted_target_events_vt) AS predicted_target_events_vt
    , sum(predicted_customer_revenue_micros_ct) AS predicted_customer_revenue_micros_ct
    , sum(predicted_customer_revenue_micros_vt) AS predicted_customer_revenue_micros_vt
  FROM funnel f
  LEFT JOIN pinpoint.public.customers cu
     ON f.customer_id = cu.id
  LEFT JOIN saleforce_data sd 
     ON f.campaign_id = sd.campaign_id
  LEFT JOIN pinpoint.public.campaigns c
     ON f.campaign_id = c.id
  LEFT JOIN pinpoint.public.apps apps
     ON f.dest_app_id = apps.id
  LEFT JOIN pinpoint.public.ad_groups ag
     ON f.ad_group_id = ag.id    
  LEFT JOIN goals
     ON f.campaign_id = goals.campaign_id
  LEFT JOIN targets 
  	 ON f.campaign_id = targets.campaign_id
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38
