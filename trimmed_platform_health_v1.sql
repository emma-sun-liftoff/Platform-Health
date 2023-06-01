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
      , sd.sales_sub_region__c AS sales_sub_region
    FROM salesforce_daily.customer_campaign__c sd 
    JOIN pinpoint.public.campaigns b      
        ON sd.campaign_id_18_digit__c = b.salesforce_campaign_id
    WHERE sd.dt = (select latest_dt FROM latest_sfdc_partition)
)
 , goals AS (
     SELECT 
     campaign_id
     , priority
     , type
     , target_value
     FROM (SELECT campaign_id, priority, type, target_value, ROW_NUMBER()
        OVER
        (PARTITION BY campaign_id
        ORDER BY campaign_id, priority ASC) as rn 
    FROM pinpoint.public.goals goals
    WHERE priority IS NOT NULL
    AND type <> 'pacing-model'
    ORDER BY 1,2 ASC)
    WHERE rn = 1
)
, treasurer_target AS (
   SELECT 
    campaign_id
    , target AS treasurer_target
   FROM pinpoint.public.campaign_treasurer_configs
)
, pinpoint_event_ids AS (
  SELECT
    id AS campaign_id
    , cpa_target_event_id
    , app_id AS dest_app_id
    , customer_id
  FROM pinpoint.public.campaigns
)
, funnel AS (
    -- fetch impressions
    SELECT
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS impression_at
    , NULL AS click_at
    , NULL AS install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
    , bid__app_platform AS platform
    , bid__customer_id AS customer_id
    , bid__app_id AS dest_app_id
    , bid__campaign_id AS campaign_id
    , bid__ad_group_type AS ad_group_type
    , bid__creative__type AS creative_type
    , CASE WHEN bid__creative__ad_format = 'video' THEN 'VAST'
           WHEN bid__creative__ad_format = 'native' THEN 'native'
           WHEN bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
           WHEN  bid__creative__ad_format = '300x250' THEN  'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , 'N/A' AS  is_viewthrough
    , CASE WHEN bid__price_data__model_type IN ('revenue','revenue-v3') THEN 'revenue' ELSE bid__price_data__model_type END AS model_type
    , bid__bid_request__non_personalized AS is_nonpersonalized
    , NULL AS click_source

    , sum(1) AS impressions
    , sum(0) AS clicks
    , sum(0) AS installs
    , sum(CAST(spend_micros AS double)/1000000) AS internal_spend
    , sum(CAST(revenue_micros AS double)/1000000) AS external_spend
    , sum(0) AS customer_revenue_d7
    , sum(0) AS target_events_d7
    , sum(0) AS target_events_first_d7
    , sum(CAST(CASE WHEN bid__price_data__model_type IN ('revenue', 'revenue-v3') THEN bid__price_data__conversion_likelihood/1000000 ELSE bid__price_data__conversion_likelihood END AS double)) AS predicted_conversion_likelihood
    , sum(CAST(bid__auction_result__winner__price_cpm_micros AS double)/1000000) AS preshaded_cpm
    , sum(CAST(bid__auction_result__winner__price_cpm_micros AS double)/1000000/CAST(bid__price_data__compensated_margin_bid_multiplier AS double)) AS private_value
    , sum(CAST(bid__price_cpm_micros AS double)/1000000) AS shaded_cpm
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
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

    UNION ALL 
    -- fetch ad clicks
    SELECT
    CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(impression__at/1000, 'UTC'))),1,19),'Z') AS impression_at
    , CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS click_at
    , NULL AS install_at
    , CONCAT(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS at
    , impression__bid__app_platform as platform
    , impression__bid__customer_id as customer_id
    , impression__bid__app_id as dest_app_id
    , impression__bid__campaign_id as campaign_id
    , impression__bid__ad_group_type AS ad_group_type
    , impression__bid__creative__type AS creative_type
    , CASE WHEN impression__bid__creative__ad_format = 'video' THEN 'VAST'
           WHEN impression__bid__creative__ad_format = 'native' THEN 'native'
           WHEN impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
           WHEN  impression__bid__creative__ad_format = '300x250' THEN 'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , 'N/A' AS is_viewthrough
    , CASE WHEN impression__bid__price_data__model_type IN ('revenue','revenue-v3') THEN 'revenue' ELSE impression__bid__price_data__model_type END AS model_type
    , impression__bid__bid_request__non_personalized AS is_nonpersonalized
    , click_source AS click_source

    , sum(0) AS impressions
    , sum(1) AS clicks
    , sum(0) AS installs
    , sum(0) AS internal_spend
    , sum(0) AS external_spend
    , sum(0) AS customer_revenue_d7
    , sum(0) AS target_events_d7
    , sum(0) AS target_events_first_d7
    , sum(0) AS predicted_conversion_likelihood
    , sum(0) AS preshaded_cpm
    , sum(0) AS private_value
    , sum(0) AS shaded_cpm
    , sum(0) AS predicted_clicks
    , sum(0) AS predicted_installs_ct
    , sum(0) AS predicted_installs_vt
    , sum(0) AS predicted_target_events_ct
    , sum(0) AS predicted_target_events_vt
    , sum(0) AS predicted_customer_revenue_micros_ct
    , sum(0) AS predicted_customer_revenue_micros_vt
    FROM rtb.ad_clicks a
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
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
    , impression__bid__customer_id as customer_id
    , impression__bid__app_id as dest_app_id
    , impression__bid__campaign_id as campaign_id
    , impression__bid__ad_group_type AS ad_group_type
    , impression__bid__creative__type AS creative_type
    , CASE WHEN impression__bid__creative__ad_format = 'video' THEN 'VAST'
           WHEN impression__bid__creative__ad_format = 'native' THEN 'native'
           WHEN impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
           WHEN  impression__bid__creative__ad_format = '300x250' THEN 'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , 'N/A' AS is_viewthrough
    , CASE WHEN impression__bid__price_data__model_type IN ('revenue','revenue-v3') THEN 'revenue' ELSE impression__bid__price_data__model_type END AS model_type
    , impression__bid__bid_request__non_personalized AS is_nonpersonalized
    , click_source AS click_source

    , sum(0) AS impressions
    , sum(1) AS clicks
    , sum(0) AS installs
    , sum(0) AS internal_spend
    , sum(0) AS external_spend
    , sum(0) AS customer_revenue_d7
    , sum(0) AS target_events_d7
    , sum(0) AS target_events_first_d7
    , sum(0) AS predicted_conversion_likelihood
    , sum(0) AS preshaded_cpm
    , sum(0) AS private_value
    , sum(0) AS shaded_cpm
    , sum(0) AS predicted_clicks
    , sum(0) AS predicted_installs_ct
    , sum(0) AS predicted_installs_vt
    , sum(0) AS predicted_target_events_ct
    , sum(0) AS predicted_target_events_vt
    , sum(0) AS predicted_customer_revenue_micros_ct
    , sum(0) AS predicted_customer_revenue_micros_vt
    FROM rtb.view_clicks a  
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
        AND has_prior_click = FALSE
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    UNION ALL    
    -- fetch matched installs
    SELECT
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__impression__at/1000, 'UTC'))),1,19),'Z') as impression_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__at/1000, 'UTC'))),1,19),'Z') AS click_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') as install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
    , ad_click__impression__bid__app_platform AS platform
    , ad_click__impression__bid__customer_id AS customer_id
    , ad_click__impression__bid__app_id AS dest_app_id
    , ad_click__impression__bid__campaign_id AS campaign_id
    , ad_click__impression__bid__ad_group_type AS ad_group_type
    , ad_click__impression__bid__creative__type AS creative_type
    , CASE WHEN ad_click__impression__bid__creative__ad_format = 'video' THEN 'VAST'
           WHEN ad_click__impression__bid__creative__ad_format = 'native' THEN 'native'
           WHEN ad_click__impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
           WHEN ad_click__impression__bid__creative__ad_format = '300x250' THEN 'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , CAST(is_viewthrough AS VARCHAR) AS is_viewthrough
    , CASE WHEN ad_click__impression__bid__price_data__model_type IN ('revenue','revenue-v3') THEN 'revenue' ELSE ad_click__impression__bid__price_data__model_type END AS model_type
    , ad_click__impression__bid__bid_request__non_personalized AS is_nonpersonalized
    , ad_click__click_source AS click_source

    , sum(0) AS impressions
    , sum(0) AS clicks
    , sum(1) AS installs
    , sum(0) AS internal_spend
    , sum(0) AS external_spend
    , sum(0) AS customer_revenue_d7
    , sum(0) AS target_events_d7
    , sum(0) AS target_events_first_d7
    , sum(0) AS predicted_conversion_likelihood
    , sum(0) AS preshaded_cpm
    , sum(0) AS private_value
    , sum(0) AS shaded_cpm
    , sum(0) AS predicted_clicks
    , sum(0) AS predicted_installs_ct
    , sum(0) AS predicted_installs_vt
    , sum(0) AS predicted_target_events_ct
    , sum(0) AS predicted_target_events_vt
    , sum(0) AS predicted_customer_revenue_micros_ct
    , sum(0) AS predicted_customer_revenue_micros_vt
    FROM rtb.matched_installs a
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

    UNION ALL
    -- fetch unmatched installs
    SELECT 
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__impression__at/1000, 'UTC'))),1,19),'Z') AS impression_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__at/1000, 'UTC'))),1,19),'Z') AS click_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
    , tracker_params__platform AS platform
    , campaigns.customer_id AS customer_id
    , campaigns.app_id AS dest_app_id
    , tracker_params__campaign_id AS campaign_id
    , ad_click__impression__bid__ad_group_type AS ad_group_type
    , 'UNMATCHED' AS creative_type
    , CASE WHEN ad_click__impression__bid__creative__ad_format = 'video' THEN 'VAST'
           WHEN ad_click__impression__bid__creative__ad_format = 'native' THEN 'native'
           WHEN ad_click__impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
           WHEN ad_click__impression__bid__creative__ad_format = '300x250' THEN 'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , CAST(is_viewthrough AS VARCHAR) AS is_viewthrough
    , CASE WHEN ad_click__impression__bid__price_data__model_type IN ('revenue','revenue-v3') THEN 'revenue' ELSE ad_click__impression__bid__price_data__model_type END AS model_type
    , ad_click__impression__bid__bid_request__non_personalized AS is_nonpersonalized
    , ad_click__click_source AS click_source    

    , sum(0) AS impressions
    , sum(0) AS clicks
    , sum(1) AS installs
    , sum(0) AS internal_spend
    , sum(0) AS external_spend
    , sum(0) AS customer_revenue_d7
    , sum(0) AS target_events_d7
    , sum(0) AS target_events_first_d7
    , sum(0) AS predicted_conversion_likelihood
    , sum(0) AS preshaded_cpm
    , sum(0) AS private_value
    , sum(0) AS shaded_cpm
    , sum(0) AS predicted_clicks
    , sum(0) AS predicted_installs_ct
    , sum(0) AS predicted_installs_vt
    , sum(0) AS predicted_target_events_ct
    , sum(0) AS predicted_target_events_vt
    , sum(0) AS predicted_customer_revenue_micros_ct
    , sum(0) AS predicted_customer_revenue_micros_vt
    FROM rtb.unmatched_installs a
    LEFT JOIN pinpoint.public.campaigns campaigns 
        ON a.tracker_params__campaign_id = campaigns.id
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
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
    , COALESCE(attribution_event__click__impression__bid__customer_id, reeng_click__impression__bid__customer_id, install__ad_click__impression__bid__customer_id) AS customer_id
    , COALESCE(attribution_event__click__impression__bid__app_id, reeng_click__impression__bid__app_id, install__ad_click__impression__bid__app_id) AS dest_app_id 
    , COALESCE(attribution_event__click__impression__bid__campaign_id, reeng_click__impression__bid__campaign_id, install__ad_click__impression__bid__campaign_id) AS campaign_id
    , COALESCE(attribution_event__click__impression__bid__ad_group_type, reeng_click__impression__bid__ad_group_type, install__ad_click__impression__bid__ad_group_type) AS ad_group_type
    , COALESCE(attribution_event__click__impression__bid__creative__type, reeng_click__impression__bid__creative__type, install__ad_click__impression__bid__creative__type) AS creative_type
    , CASE WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'video' THEN 'VAST'
           WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'native' THEN 'native'
           WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) IN ('320x50', '728x90') THEN 'banner'
           WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = '300x250' THEN  'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , CAST(is_viewthrough AS VARCHAR) AS is_viewthrough
    , CASE WHEN COALESCE(attribution_event__click__impression__bid__price_data__model_type, reeng_click__impression__bid__price_data__model_type, install__ad_click__impression__bid__price_data__model_type) IN ('revenue','revenue-v3') THEN 'revenue' ELSE COALESCE(attribution_event__click__impression__bid__price_data__model_type, reeng_click__impression__bid__price_data__model_type, install__ad_click__impression__bid__price_data__model_type) END AS model_type 
    , COALESCE(attribution_event__click__impression__bid__bid_request__non_personalized, reeng_click__impression__bid__bid_request__non_personalized, install__ad_click__impression__bid__bid_request__non_personalized) AS is_nonpersonalized
    , COALESCE(attribution_event__click__click_source, reeng_click__click_source, install__ad_click__click_source) AS click_source    

    , sum(0) AS impressions
    , sum(0) AS clicks
    , sum(0) AS installs
    , sum(0) AS internal_spend
    , sum(0) AS external_spend
    , sum(IF(customer_revenue_micros > -100000000000 AND customer_revenue_micros < 100000000000, customer_revenue_micros, 0)) AS customer_revenue_d7
    , sum(IF(pinpoint_event_ids.cpa_target_event_id = custom_event_id,1,0)) AS target_events_d7
    , sum(IF(pinpoint_event_ids.cpa_target_event_id = custom_event_id, IF(first_occurrence,1,0),0)) AS target_events_first_d7
    , sum(0) AS predicted_conversion_likelihood
    , sum(0) AS preshaded_cpm
    , sum(0) AS private_value
    , sum(0) AS shaded_cpm
    , sum(0) AS predicted_clicks
    , sum(0) AS predicted_installs_ct
    , sum(0) AS predicted_installs_vt
    , sum(0) AS predicted_target_events_ct
    , sum(0) AS predicted_target_events_vt
    , sum(0) AS predicted_customer_revenue_micros_ct
    , sum(0) AS predicted_customer_revenue_micros_vt
    FROM rtb.matched_app_events a
    LEFT JOIN pinpoint_event_ids
        ON COALESCE(attribution_event__click__impression__bid__campaign_id, reeng_click__impression__bid__campaign_id, install__ad_click__impression__bid__campaign_id) = pinpoint_event_ids.campaign_id
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
    AND for_reporting = TRUE
    AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

    UNION ALL 
    -- fetch unmatched app events
    SELECT 
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(COALESCE(attribution_event__click__impression__at, reeng_click__impression__at, install__ad_click__impression__at)/1000, 'UTC'))),1,19),'Z') AS impression_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(COALESCE(attribution_event__click__at, reeng_click__at, install__ad_click__at)/1000, 'UTC'))),1,19),'Z') AS click_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(install__at/1000, 'UTC'))),1,19),'Z') AS install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
    , tracker_params__platform AS platform 
    , pinpoint_event_ids.customer_id AS customer_id
    , pinpoint_event_ids.dest_app_id AS dest_app_id 
    , tracker_params__campaign_id AS campaign_id
    , COALESCE(attribution_event__click__impression__bid__ad_group_type, reeng_click__impression__bid__ad_group_type, install__ad_click__impression__bid__ad_group_type) AS ad_group_type
    , 'UNMATCHED' as creative_type
    , CASE WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'video' THEN 'VAST'
           WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'native' THEN 'native'
           WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) IN ('320x50', '728x90') THEN 'banner'
           WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = '300x250' THEN  'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , CAST(is_viewthrough AS VARCHAR)
    , CASE WHEN COALESCE(attribution_event__click__impression__bid__price_data__model_type, reeng_click__impression__bid__price_data__model_type, install__ad_click__impression__bid__price_data__model_type) IN ('revenue','revenue-v3') THEN 'revenue' ELSE COALESCE(attribution_event__click__impression__bid__price_data__model_type, reeng_click__impression__bid__price_data__model_type, install__ad_click__impression__bid__price_data__model_type) END AS model_type 
    , COALESCE(attribution_event__click__impression__bid__bid_request__non_personalized, reeng_click__impression__bid__bid_request__non_personalized, install__ad_click__impression__bid__bid_request__non_personalized) AS is_nonpersonalized
    , COALESCE(attribution_event__click__click_source, reeng_click__click_source, install__ad_click__click_source) AS click_source  

    , sum(0) AS impressions
    , sum(0) AS clicks
    , sum(0) AS installs
    , sum(0) AS internal_spend
    , sum(0) AS external_spend
    , sum(IF(customer_revenue_micros > -100000000000 AND customer_revenue_micros < 100000000000, customer_revenue_micros, 0)) AS customer_revenue_d7
    , sum(IF(pinpoint_event_ids.cpa_target_event_id = custom_event_id,1,0)) AS target_events_d7
    , sum(IF(pinpoint_event_ids.cpa_target_event_id = custom_event_id, IF(first_occurrence,1,0),0)) AS target_events_first_d7
    , sum(0) AS predicted_conversion_likelihood
    , sum(0) AS preshaded_cpm
    , sum(0) AS private_value
    , sum(0) AS shaded_cpm
    , sum(0) AS predicted_clicks
    , sum(0) AS predicted_installs_ct
    , sum(0) AS predicted_installs_vt
    , sum(0) AS predicted_target_events_ct
    , sum(0) AS predicted_target_events_vt
    , sum(0) AS predicted_customer_revenue_micros_ct
    , sum(0) AS predicted_customer_revenue_micros_vt
    FROM rtb.unmatched_app_events a
    LEFT JOIN pinpoint_event_ids
        ON a.tracker_params__campaign_id = pinpoint_event_ids.campaign_id
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)         
        
    SELECT
    f.impression_at
    , f.install_at
    , f.at
    , f.platform
    , f.customer_id
    , f.dest_app_id
    , f.campaign_id 
    , f.ad_group_type
    , f.creative_type
    , f.ad_format
    , f.is_viewthrough
    , f.model_type
    , f.is_nonpersonalized
    , f.click_source   
    , sd.service_level   
    , cu.company AS customer_name
    , apps.display_name AS dest_app_name 
    , sd.sales_region AS campaign_sales_region
    , sd.sales_sub_region AS campaign_sales_sub_region
    , goals.type AS goal_type_3
    , goals.target_value AS goal_3_value
    , treasurer_target.treasurer_target AS treasurer_target
    , CASE WHEN f.creative_type = 'VAST' AND apps.video_viewthrough_enabled = TRUE THEN TRUE
           WHEN f.ad_format in ('banner','mrec') AND f.creative_type = 'HTML' AND apps.banner_viewthrough_enabled = TRUE THEN TRUE
           WHEN f.ad_format = 'interstitial' AND f.creative_type = 'HTML' AND apps.interstitial_viewthrough_enabled = TRUE THEN TRUE
           WHEN f.ad_format = 'native' AND apps.native_viewthrough_enabled = TRUE THEN TRUE  
           ELSE FALSE END AS viewthrough_enabled
   
    , sum(f.impressions) AS impressions
    , sum(f.clicks) AS clicks
    , sum(f.installs) AS installs
    , sum(f.internal_spend) AS internal_spend
    , sum(f.external_spend) AS external_spend
    , sum(IF(from_iso8601_timestamp(f.at) - from_iso8601_timestamp(IF(f.ad_group_type='reengagement', f.click_at, f.install_at)) < INTERVAL '7' DAY, CAST(f.customer_revenue_d7 AS double)/1000000, 0)) AS customer_revenue_d7
    , sum(IF(from_iso8601_timestamp(f.at) - from_iso8601_timestamp(IF(f.ad_group_type='reengagement', f.click_at, f.install_at)) < INTERVAL '7' DAY, f.target_events_d7, 0)) AS target_events_d7
    , sum(IF(from_iso8601_timestamp(f.at) - from_iso8601_timestamp(IF(f.ad_group_type='reengagement', f.click_at, f.install_at)) < INTERVAL '7' DAY, f.target_events_first_d7, 0)) AS target_events_first_d7 
    , sum(predicted_conversion_likelihood) AS predicted_conversion_likelihood
    , sum(preshaded_cpm) AS preshaded_cpm
    , sum(private_value) AS private_value
    , sum(shaded_cpm) AS shaded_cpm
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
  LEFT JOIN pinpoint.public.apps apps
     ON f.dest_app_id = apps.id   
  LEFT JOIN goals
     ON f.campaign_id = goals.campaign_id
  LEFT JOIN treasurer_target 
     ON f.campaign_id = treasurer_target.campaign_id
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
