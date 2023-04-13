WITH latest_sfdc_partition AS (
    SELECT MAX(dt) AS latest_dt 
    FROM salesforce_daily.customer_campaign__c  
    WHERE from_iso8601_timestamp(dt) >= CURRENT_TIMESTAMP - INTERVAL '2' DAY
)
 , saleforce_data AS (
    SELECT 
      b.id AS campaign_id
      , sd.sales_region__c as sales_region
      , sd.service_level__c AS service_level
    FROM salesforce_daily.customer_campaign__c sd 
    JOIN pinpoint.public.campaigns b      
        ON sd.campaign_id_18_digit__c = b.salesforce_campaign_id
    WHERE sd.dt = (select latest_dt FROM latest_sfdc_partition)
)
, convx_buckets AS (
  SELECT 
  platform
  , model_type
  , exchange
  , ad_group_id
  , p.low
  , p.high
  , p.name
  FROM product_analytics.convx_likelihood_bucket_v1
  CROSS JOIN UNNEST(percentiles) AS p
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
    WHERE dt BETWEEN '2023-04-12T01' AND '2023-04-12T06'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

    UNION ALL 
    -- fetch ad clicks
    SELECT
    CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(impression__at/1000, 'UTC'))),1,19),'Z') AS impression_at
    , CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS click_at
    , NULL AS install_at
    , CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
    , impression__bid__app_platform as platform
    , impression__bid__bid_request__exchange as exchange
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
    , impression__bid__price_data__model_type as model_type
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
    WHERE dt BETWEEN '2023-04-12T01' AND '2023-04-12T06'
        AND has_prior_click = FALSE
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    
    UNION ALL 
     -- fetch view clicks
    SELECT
    CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(impression__at/1000, 'UTC'))),1,19),'Z') AS impression_at
    , CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS click_at
    , NULL AS install_at
    , CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
    , impression__bid__app_platform as platform
    , impression__bid__bid_request__exchange as exchange
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
    , impression__bid__price_data__model_type as model_type
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
    WHERE dt BETWEEN '2023-04-12T01' AND '2023-04-12T06'
        AND has_prior_click = FALSE
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    
    UNION ALL    
    -- fetch installs
    SELECT
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__impression__at/1000, 'UTC'))),1,19),'Z') as impression_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__at/1000, 'UTC'))),1,19),'Z') AS click_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') as install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
    , ad_click__impression__bid__app_platform AS platform
    , ad_click__impression__bid__bid_request__exchange AS exchange
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
    WHERE dt BETWEEN '2023-04-12T01' AND '2023-04-12T06'
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

    UNION ALL 
    -- to fetch down funnel data (we are using 7d cohorted by installs data)

    SELECT
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(COALESCE(attribution_event__click__impression__at, reeng_click__impression__at, install__ad_click__impression__at)/1000, 'UTC'))),1,19),'Z') AS impression_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(COALESCE(attribution_event__click__at, reeng_click__at, install__ad_click__at)/1000, 'UTC'))),1,19),'Z') AS click_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(install__at/1000, 'UTC'))),1,19),'Z') AS install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
    , COALESCE(attribution_event__click__impression__bid__app_platform, reeng_click__impression__bid__app_platform, install__ad_click__impression__bid__app_platform) AS platform
    , COALESCE(attribution_event__click__impression__bid__bid_request__exchange, reeng_click__impression__bid__bid_request__exchange, install__ad_click__impression__bid__bid_request__exchange) AS exchange
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
    WHERE dt BETWEEN '2023-04-12T01' AND '2023-04-12T06'
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
 SELECT
    f.impression_at
    , f.click_at
    , f.install_at
    , f.at
    , f.platform
    , f.exchange
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
    , cb.name AS convx_percentile
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
  LEFT JOIN convx_buckets cb
     ON f.platform = cb.platform
        AND f.model_type = cb.model_type
        AND f.ad_group_id = cb.ad_group_id
        AND f.exchange = cb.exchange
        AND f.predicted_conversion_likelihood >= cb.low 
        AND (cb.high IS NULL OR f.predicted_conversion_likelihood < cb.high)

  WHERE f.ad_group_id = 129051
    AND f.exchange = 'SMAATO'
    AND f.impression_at >= '2023-04-12T04' and f.impression_at <= '2023-04-12T10'
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
