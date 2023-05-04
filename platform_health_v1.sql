
  WITH percentile_split AS (
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
        ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.9999]) AS covx_likelihood_percentile
    , approx_percentile(bid__auction_result__winner__price_cpm_micros,
        ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.9999]) AS preshaded_price_percentile
    , approx_percentile(bid__price_data__ad_group_cpx_bid_micros,
        ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.9999]) AS bid_target_percentile
  FROM rtb.impressions_with_bids
  WHERE dt >= '2023-04-12T01' AND dt < '2023-04-12T03'
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
         ARRAY[0] || covx_likelihood_percentile, covx_likelihood_percentile || CAST(ARRAY[null] as ARRAY(DOUBLE)),
         ARRAY['0-10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', 
	 '70-80', '80-90', '90-95', '95-99', '99-99.99', '99.99-100']
        ) AS ARRAY(ROW(low DOUBLE, high DOUBLE, name VARCHAR))) AS covx_likelihood_bucket
  , CAST(
        zip(
         ARRAY[0] || preshaded_price_percentile, preshaded_price_percentile || CAST(ARRAY[null] as ARRAY(DOUBLE)),
         ARRAY['0-10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', 
	 '70-80', '80-90', '90-95', '95-99', '99-99.99', '99.99-100']
        ) AS ARRAY(ROW(low DOUBLE, high DOUBLE, name VARCHAR))) AS preshaded_price_bucket
  , CAST(
        zip(
         ARRAY[0] || bid_target_percentile, bid_target_percentile || CAST(ARRAY[null] as ARRAY(DOUBLE)),
         ARRAY['0-10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', 
	 '70-80', '80-90', '90-95', '95-99', '99-99.99', '99.99-100']
        ) AS ARRAY(ROW(low DOUBLE, high DOUBLE, name VARCHAR))) AS bid_target_bucket
 FROM percentile_split
 )
, buckets AS (
  SELECT 
  platform
  , model_type
  , exchange
  , ad_format
  , clb.low AS convx_percentile_low
  , clb.high AS convx_percentile_high
  , clb.name AS convx_percentile
  , ppb.low AS preshaded_price_percentile_low
  , ppb.high AS preshaded_price_percentile_high
  , ppb.name AS preshaded_price_percentile
  , btb.low AS bid_target_percentile_low
  , btb.high AS bid_target_percentile_high
  , btb.name AS bid_target_percentile
  FROM percentile_bucket
  CROSS JOIN UNNEST(covx_likelihood_bucket) AS clb
  CROSS JOIN UNNEST(preshaded_price_bucket) AS ppb
  CROSS JOIN UNNEST(bid_target_bucket) AS btb
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
     SELECT campaign_id, priority, type, target_value
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
, targets AS (
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
    , b.convx_percentile
    , b.convx_percentile_low
    , b.convx_percentile_high
    , b.preshaded_price_percentile_low
    , b.preshaded_price_percentile_high
    , b.preshaded_price_percentile
    , b.bid_target_percentile_low
    , b.bid_target_percentile_high
    , b.bid_target_percentile
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
    , sum(bid__auction_result__winner__price_cpm_micros) AS preshaded_cpm_micros
    , sum(CAST(bid__auction_result__winner__price_cpm_micros AS double)/CAST(bid__price_data__compensated_margin_bid_multiplier AS double)) AS private_cpm_micros
    , sum(bid__price_data__ad_group_cpx_bid_micros) AS bid_target_total
    , sum(bid__price_data__effective_cpx_bid_micros) AS effective_bid_target 
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
    JOIN buckets b
        ON a.bid__app_platform = b.platform
            AND a.bid__price_data__model_type = b.model_type
            AND (CASE WHEN bid__creative__ad_format = 'video' THEN 'VAST'
                      WHEN bid__creative__ad_format = 'native' THEN 'native'
                      WHEN bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
                      WHEN bid__creative__ad_format = '300x250' THEN  'mrec'
                      ELSE 'html-interstitial' END) = b.ad_format
            AND a.bid__bid_request__exchange = b.exchange
            AND a.bid__price_data__conversion_likelihood >= b.convx_percentile_low
            AND (b.convx_percentile_high IS NULL OR a.bid__price_data__conversion_likelihood < b.convx_percentile_high)
            AND a.bid__auction_result__winner__price_cpm_micros >= b.preshaded_price_percentile_low
            AND (b.preshaded_price_percentile_high IS NULL OR a.bid__auction_result__winner__price_cpm_micros < b.preshaded_price_percentile_high)            
            AND a.bid__price_data__ad_group_cpx_bid_micros >= b.bid_target_percentile_low
            AND (b.bid_target_percentile_high IS NULL OR a.bid__price_data__ad_group_cpx_bid_micros < b.bid_target_percentile_high)   
    WHERE dt >= '2023-04-12T01' AND dt < '2023-04-12T03'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26

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
    , b.convx_percentile
    , b.convx_percentile_low
    , b.convx_percentile_high
    , b.preshaded_price_percentile_low
    , b.preshaded_price_percentile_high
    , b.preshaded_price_percentile
    , b.bid_target_percentile_low
    , b.bid_target_percentile_high
    , b.bid_target_percentile
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
    , sum(0) AS preshaded_cpm_micros
    , sum(0) AS private_cpm_micros
    , sum(0) AS bid_target_total
    , sum(0) AS effective_bid_target
    , sum(0) AS predicted_clicks
    , sum(0) AS predicted_installs_ct
    , sum(0) AS predicted_installs_vt
    , sum(0) AS predicted_target_events_ct
    , sum(0) AS predicted_target_events_vt
    , sum(0) AS predicted_customer_revenue_micros_ct
    , sum(0) AS predicted_customer_revenue_micros_vt
    FROM rtb.ad_clicks a
    JOIN buckets b
        ON a.impression__bid__app_platform = b.platform
            AND a.impression__bid__price_data__model_type = b.model_type
            AND (CASE WHEN impression__bid__creative__ad_format = 'video' THEN 'VAST'
                      WHEN impression__bid__creative__ad_format = 'native' THEN 'native'
                      WHEN impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
                      WHEN impression__bid__creative__ad_format = '300x250' THEN 'mrec'
                      ELSE 'html-interstitial' END) = b.ad_format
            AND a.impression__bid__bid_request__exchange = b.exchange
            AND a.impression__bid__price_data__conversion_likelihood >= b.convx_percentile_low 
            AND (b.convx_percentile_high IS NULL OR a.impression__bid__price_data__conversion_likelihood < b.convx_percentile_high)
            AND a.impression__bid__auction_result__winner__price_cpm_micros >= b.preshaded_price_percentile_low
            AND (b.preshaded_price_percentile_high IS NULL OR a.impression__bid__auction_result__winner__price_cpm_micros < b.preshaded_price_percentile_high)            
            AND a.impression__bid__price_data__ad_group_cpx_bid_micros >= b.bid_target_percentile_low
            AND (b.bid_target_percentile_high IS NULL OR a.impression__bid__price_data__ad_group_cpx_bid_micros < b.bid_target_percentile_high)   
    WHERE dt >= '2023-04-12T01' AND dt < '2023-04-12T03'
        AND has_prior_click = FALSE
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
    
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
    , b.convx_percentile
    , b.convx_percentile_low
    , b.convx_percentile_high
    , b.preshaded_price_percentile_low
    , b.preshaded_price_percentile_high
    , b.preshaded_price_percentile
    , b.bid_target_percentile_low
    , b.bid_target_percentile_high
    , b.bid_target_percentile
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
    , sum(0) AS preshaded_cpm_micros
    , sum(0) AS private_cpm_micros
    , sum(0) AS bid_target_total
    , sum(0) AS effective_bid_target
    , sum(0) AS predicted_clicks
    , sum(0) AS predicted_installs_ct
    , sum(0) AS predicted_installs_vt
    , sum(0) AS predicted_target_events_ct
    , sum(0) AS predicted_target_events_vt
    , sum(0) AS predicted_customer_revenue_micros_ct
    , sum(0) AS predicted_customer_revenue_micros_vt
    FROM rtb.view_clicks a
    JOIN buckets b
        ON a.impression__bid__app_platform = b.platform
            AND a.impression__bid__price_data__model_type = b.model_type
            AND (CASE WHEN impression__bid__creative__ad_format = 'video' THEN 'VAST'
                      WHEN impression__bid__creative__ad_format = 'native' THEN 'native'
                      WHEN impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
                      WHEN impression__bid__creative__ad_format = '300x250' THEN 'mrec'
                      ELSE 'html-interstitial' END) = b.ad_format
            AND a.impression__bid__bid_request__exchange = b.exchange
            AND a.impression__bid__price_data__conversion_likelihood >= b.convx_percentile_low 
            AND (b.convx_percentile_high IS NULL OR a.impression__bid__price_data__conversion_likelihood < b.convx_percentile_high)
            AND a.impression__bid__auction_result__winner__price_cpm_micros >= b.preshaded_price_percentile_low
            AND (b.preshaded_price_percentile_high IS NULL OR a.impression__bid__auction_result__winner__price_cpm_micros < b.preshaded_price_percentile_high)            
            AND a.impression__bid__price_data__ad_group_cpx_bid_micros >= b.bid_target_percentile_low
            AND (b.bid_target_percentile_high IS NULL OR a.impression__bid__price_data__ad_group_cpx_bid_micros < b.bid_target_percentile_high)   
    WHERE dt >= '2023-04-12T01' AND dt < '2023-04-12T03'
        AND has_prior_click = FALSE
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
    
    UNION ALL    
    -- fetch matched installs
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
    , b.convx_percentile
    , b.convx_percentile_low
    , b.convx_percentile_high
    , b.preshaded_price_percentile_low
    , b.preshaded_price_percentile_high
    , b.preshaded_price_percentile
    , b.bid_target_percentile_low
    , b.bid_target_percentile_high
    , b.bid_target_percentile
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
    , sum(0) AS preshaded_cpm_micros
    , sum(0) AS private_cpm_micros
    , sum(0) AS bid_target_total
    , sum(0) AS effective_bid_target
    , sum(0) AS predicted_clicks
    , sum(0) AS predicted_installs_ct
    , sum(0) AS predicted_installs_vt
    , sum(0) AS predicted_target_events_ct
    , sum(0) AS predicted_target_events_vt
    , sum(0) AS predicted_customer_revenue_micros_ct
    , sum(0) AS predicted_customer_revenue_micros_vt
    FROM rtb.matched_installs a
    JOIN buckets b
        ON a.ad_click__impression__bid__app_platform = b.platform
            AND a.ad_click__impression__bid__price_data__model_type = b.model_type
            AND (CASE WHEN ad_click__impression__bid__creative__ad_format = 'video' THEN 'VAST'
                      WHEN ad_click__impression__bid__creative__ad_format = 'native' THEN 'native'
                      WHEN ad_click__impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
                      WHEN ad_click__impression__bid__creative__ad_format = '300x250' THEN 'mrec'
                      ELSE 'html-interstitial' END) = b.ad_format
            AND a.ad_click__impression__bid__bid_request__exchange = b.exchange
            AND a.ad_click__impression__bid__price_data__conversion_likelihood >= b.convx_percentile_low 
            AND (b.convx_percentile_high IS NULL OR a.ad_click__impression__bid__price_data__conversion_likelihood < b.convx_percentile_high)
            AND a.ad_click__impression__bid__auction_result__winner__price_cpm_micros >= b.preshaded_price_percentile_low
            AND (b.preshaded_price_percentile_high IS NULL OR a.ad_click__impression__bid__auction_result__winner__price_cpm_micros < b.preshaded_price_percentile_high)            
            AND a.ad_click__impression__bid__price_data__ad_group_cpx_bid_micros >= b.bid_target_percentile_low
            AND (b.bid_target_percentile_high IS NULL OR a.ad_click__impression__bid__price_data__ad_group_cpx_bid_micros < b.bid_target_percentile_high)   
    WHERE dt >= '2023-04-12T01' AND dt < '2023-04-12T03'
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26

    UNION ALL
    -- fetch unmatched installs
    SELECT 
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__impression__at/1000, 'UTC'))),1,19),'Z') AS impression_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__at/1000, 'UTC'))),1,19),'Z') AS click_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
    , tracker_params__platform AS platform
    , 'UNMATCHED' AS exchange
    , COALESCE(ad_click__geo__country, geo__country) AS country
    , campaigns.customer_id AS customer_id
    , campaigns.app_id AS dest_app_id
    , tracker_params__campaign_id AS campaign_id
    , ad_click__impression__bid__ad_group_id AS ad_group_id
    , ad_click__impression__bid__ad_group_type AS ad_group_type
    , 'UNMATCHED' AS creative_type
    , CASE WHEN ad_click__impression__bid__creative__ad_format = 'video' THEN 'VAST'
           WHEN ad_click__impression__bid__creative__ad_format = 'native' THEN 'native'
           WHEN ad_click__impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
           WHEN ad_click__impression__bid__creative__ad_format = '300x250' THEN 'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , CAST(is_viewthrough AS VARCHAR) AS is_viewthrough
    , ad_click__impression__bid__price_data__model_type AS model_type
    , b.convx_percentile
    , b.convx_percentile_low
    , b.convx_percentile_high
    , b.preshaded_price_percentile_low
    , b.preshaded_price_percentile_high
    , b.preshaded_price_percentile
    , b.bid_target_percentile_low
    , b.bid_target_percentile_high
    , b.bid_target_percentile    
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
    , sum(0) AS preshaded_cpm_micros
    , sum(0) AS private_cpm_micros
    , sum(0) AS bid_target_total
    , sum(0) AS effective_bid_target
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
    JOIN buckets b
        ON a.tracker_params__platform = b.platform
            AND a.ad_click__impression__bid__price_data__model_type = b.model_type
            AND (CASE WHEN ad_click__impression__bid__creative__ad_format = 'video' THEN 'VAST'
                      WHEN ad_click__impression__bid__creative__ad_format = 'native' THEN 'native'
                      WHEN ad_click__impression__bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
                      WHEN ad_click__impression__bid__creative__ad_format = '300x250' THEN 'mrec'
                      ELSE 'html-interstitial' END) = b.ad_format
            AND a.ad_click__impression__bid__price_data__conversion_likelihood >= b.convx_percentile_low 
            AND (b.convx_percentile_high IS NULL OR a.ad_click__impression__bid__price_data__conversion_likelihood < b.convx_percentile_high)
            AND a.ad_click__impression__bid__auction_result__winner__price_cpm_micros >= b.preshaded_price_percentile_low
            AND (b.preshaded_price_percentile_high IS NULL OR a.ad_click__impression__bid__auction_result__winner__price_cpm_micros < b.preshaded_price_percentile_high)            
            AND a.ad_click__impression__bid__price_data__ad_group_cpx_bid_micros >= b.bid_target_percentile_low
            AND (b.bid_target_percentile_high IS NULL OR a.ad_click__impression__bid__price_data__ad_group_cpx_bid_micros < b.bid_target_percentile_high)   
    WHERE dt >= '2023-04-12T01' AND dt < '2023-04-12T03'
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26    
    
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
    , b.convx_percentile
    , b.convx_percentile_low
    , b.convx_percentile_high
    , b.preshaded_price_percentile_low
    , b.preshaded_price_percentile_high
    , b.preshaded_price_percentile
    , b.bid_target_percentile_low
    , b.bid_target_percentile_high
    , b.bid_target_percentile    
    , COALESCE(attribution_event__click__click_source, reeng_click__click_source, install__ad_click__click_source) AS click_source
    , sum(0) AS impressions
    , sum(0) AS clicks
    , sum(0) AS installs
    , sum(0) AS internal_spend_micros
    , sum(0) AS external_spend_micros
    , sum(IF(customer_revenue_micros > -100000000000 AND customer_revenue_micros < 100000000000, customer_revenue_micros, 0)) AS customer_revenue_micros_d7
    , sum(IF(pinpoint_event_ids.cpa_target_event_id = custom_event_id,1,0)) AS target_events_d7
    , sum(IF(pinpoint_event_ids.cpa_target_event_id = custom_event_id, IF(first_occurrence,1,0),0)) AS target_events_first_d7
    , sum(0) AS predicted_conversion_likelihood
    , sum(0) AS preshaded_cpm_micros
    , sum(0) AS private_cpm_micros
    , sum(0) AS bid_target_total
    , sum(0) AS effective_bid_target
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
    JOIN buckets b
        ON COALESCE(install__ad_click__impression__bid__app_platform, reeng_click__impression__bid__app_platform, attribution_event__click__impression__bid__app_platform) = b.platform
            AND COALESCE(install__ad_click__impression__bid__price_data__model_type, reeng_click__impression__bid__price_data__model_type, attribution_event__click__impression__bid__price_data__model_type) = b.model_type
            AND (CASE WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'video' THEN 'VAST'
                      WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'native' THEN 'native'
                      WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) IN ('320x50', '728x90') THEN 'banner'
                      WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = '300x250' THEN  'mrec'
                      ELSE 'html-interstitial' END) = b.ad_format
            AND COALESCE(install__ad_click__impression__bid__bid_request__exchange, reeng_click__impression__bid__bid_request__exchange, attribution_event__click__impression__bid__bid_request__exchange) = b.exchange
            AND COALESCE(install__ad_click__impression__bid__price_data__conversion_likelihood, reeng_click__impression__bid__price_data__conversion_likelihood, attribution_event__click__impression__bid__price_data__conversion_likelihood) >= b.convx_percentile_low 
            AND (b.convx_percentile_high IS NULL OR COALESCE(install__ad_click__impression__bid__price_data__conversion_likelihood, reeng_click__impression__bid__price_data__conversion_likelihood, attribution_event__click__impression__bid__price_data__conversion_likelihood) < b.convx_percentile_high)
            AND COALESCE(install__ad_click__impression__bid__auction_result__winner__price_cpm_micros, reeng_click__impression__bid__auction_result__winner__price_cpm_micros, attribution_event__click__impression__bid__price_data__ad_group_cpx_bid_micros) >= b.preshaded_price_percentile_low
            AND (b.preshaded_price_percentile_high IS NULL OR COALESCE(install__ad_click__impression__bid__auction_result__winner__price_cpm_micros, reeng_click__impression__bid__auction_result__winner__price_cpm_micros, attribution_event__click__impression__bid__price_data__ad_group_cpx_bid_micros) < b.preshaded_price_percentile_high)            
            AND COALESCE(install__ad_click__impression__bid__price_data__ad_group_cpx_bid_micros, reeng_click__impression__bid__price_data__ad_group_cpx_bid_micros, attribution_event__click__impression__bid__price_data__ad_group_cpx_bid_micros) >= b.bid_target_percentile_low
            AND (b.bid_target_percentile_high IS NULL OR COALESCE(install__ad_click__impression__bid__price_data__ad_group_cpx_bid_micros, reeng_click__impression__bid__price_data__ad_group_cpx_bid_micros, attribution_event__click__impression__bid__price_data__ad_group_cpx_bid_micros) < b.bid_target_percentile_high)  
    WHERE dt >= '2023-04-12T01' AND dt < '2023-04-12T03'
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26

    UNION ALL 
    -- fetch unmatched app events
    SELECT 
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(COALESCE(attribution_event__click__impression__at, reeng_click__impression__at, install__ad_click__impression__at)/1000, 'UTC'))),1,19),'Z') AS impression_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(COALESCE(attribution_event__click__at, reeng_click__at, install__ad_click__at)/1000, 'UTC'))),1,19),'Z') AS click_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(install__at/1000, 'UTC'))),1,19),'Z') AS install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
    , tracker_params__platform AS platform 
    , 'UNMATCHED' as exchange
    , COALESCE(install__geo__country, geo__country) AS country
    , pinpoint_event_ids.customer_id AS customer_id
    , pinpoint_event_ids.dest_app_id AS dest_app_id 
    , tracker_params__campaign_id AS campaign_id
    , COALESCE(attribution_event__click__impression__bid__ad_group_id, reeng_click__impression__bid__ad_group_id, install__ad_click__impression__bid__ad_group_id) AS ad_group_id
    , COALESCE(attribution_event__click__impression__bid__ad_group_type, reeng_click__impression__bid__ad_group_type, install__ad_click__impression__bid__ad_group_type) AS ad_group_type
    , 'UNMATCHED' as creative_type
	, CASE WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'video' THEN 'VAST'
	       WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'native' THEN 'native'
	       WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) IN ('320x50', '728x90') THEN 'banner'
	       WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = '300x250' THEN  'mrec'
	       ELSE 'html-interstitial' END AS ad_format
    , CAST(is_viewthrough AS VARCHAR)
    , COALESCE(attribution_event__click__impression__bid__price_data__model_type, reeng_click__impression__bid__price_data__model_type, install__ad_click__impression__bid__price_data__model_type) AS model_type         
    , b.convx_percentile
    , b.convx_percentile_low
    , b.convx_percentile_high
    , b.preshaded_price_percentile_low
    , b.preshaded_price_percentile_high
    , b.preshaded_price_percentile
    , b.bid_target_percentile_low
    , b.bid_target_percentile_high
    , b.bid_target_percentile    
    , COALESCE(attribution_event__click__click_source, reeng_click__click_source, install__ad_click__click_source) AS click_source

    , sum(0) AS impressions
    , sum(0) AS clicks
    , sum(0) AS installs
    , sum(0) AS internal_spend_micros
    , sum(0) AS external_spend_micros
    , sum(IF(customer_revenue_micros > -100000000000 AND customer_revenue_micros < 100000000000, customer_revenue_micros, 0)) AS customer_revenue_micros_d7
    , sum(IF(pinpoint_event_ids.cpa_target_event_id = custom_event_id,1,0)) AS target_events_d7
    , sum(IF(pinpoint_event_ids.cpa_target_event_id = custom_event_id, IF(first_occurrence,1,0),0)) AS target_events_first_d7
    , sum(0) AS predicted_conversion_likelihood
    , sum(0) AS preshaded_cpm_micros
    , sum(0) AS private_cpm_micros
    , sum(0) AS bid_target_total
    , sum(0) AS effective_bid_target
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
    JOIN buckets b
        ON a.tracker_params__platform = b.platform
            AND COALESCE(attribution_event__click__impression__bid__price_data__model_type, reeng_click__impression__bid__price_data__model_type, install__ad_click__impression__bid__price_data__model_type) = b.model_type
            AND (CASE WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'video' THEN 'VAST'
                      WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = 'native' THEN 'native'
                      WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) IN ('320x50', '728x90') THEN 'banner'
                      WHEN COALESCE(install__ad_click__impression__bid__creative__ad_format,reeng_click__impression__bid__creative__ad_format,attribution_event__click__impression__bid__creative__ad_format) = '300x250' THEN  'mrec'
                      ELSE 'html-interstitial' END) = b.ad_format
            AND COALESCE(install__ad_click__impression__bid__price_data__conversion_likelihood, reeng_click__impression__bid__price_data__conversion_likelihood, attribution_event__click__impression__bid__price_data__conversion_likelihood) >= b.convx_percentile_low 
            AND (b.convx_percentile_high IS NULL OR COALESCE(install__ad_click__impression__bid__price_data__conversion_likelihood, reeng_click__impression__bid__price_data__conversion_likelihood, attribution_event__click__impression__bid__price_data__conversion_likelihood) < b.convx_percentile_high)
            AND COALESCE(install__ad_click__impression__bid__auction_result__winner__price_cpm_micros, reeng_click__impression__bid__auction_result__winner__price_cpm_micros, attribution_event__click__impression__bid__price_data__ad_group_cpx_bid_micros) >= b.preshaded_price_percentile_low
            AND (b.preshaded_price_percentile_high IS NULL OR COALESCE(install__ad_click__impression__bid__auction_result__winner__price_cpm_micros, reeng_click__impression__bid__auction_result__winner__price_cpm_micros, attribution_event__click__impression__bid__price_data__ad_group_cpx_bid_micros) < b.preshaded_price_percentile_high)            
            AND COALESCE(install__ad_click__impression__bid__price_data__ad_group_cpx_bid_micros, reeng_click__impression__bid__price_data__ad_group_cpx_bid_micros, attribution_event__click__impression__bid__price_data__ad_group_cpx_bid_micros) >= b.bid_target_percentile_low
            AND (b.bid_target_percentile_high IS NULL OR COALESCE(install__ad_click__impression__bid__price_data__ad_group_cpx_bid_micros, reeng_click__impression__bid__price_data__ad_group_cpx_bid_micros, attribution_event__click__impression__bid__price_data__ad_group_cpx_bid_micros) < b.bid_target_percentile_high)  
    WHERE dt >= '2023-04-12T01' AND dt < '2023-04-12T03'
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26   
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
    , apps.display_name AS dest_app_name
    , f.campaign_id
    , c.display_name AS campaign_name
    , c.current_optimization_state
    , targets.treasurer_target AS treasurer_target
    , f.ad_group_type
    , ag.exploratory as exploratory_ad_group
    , ag.bid_type as ad_group_bid_type
    , f.ad_group_id
    , ag.display_name as ad_group_name
    , f.ad_format
    , f.model_type
  	, IF(trackers.name IS NOT NULL, trackers.name, 'N/A') AS campaign_tracker
  	, IF(trackers.name = 'apple-skan', 'SKAN', IF(trackers.name = 'no-tracker', 'NON-MEASURABLE', IF(f.campaign_id IS NULL, 'N/A', 'MMP'))) AS campaign_tracker_type
    , f.creative_type
    , f.click_source
    , f.is_viewthrough 
    , IF(c.vt_cap > 0, TRUE, FALSE) AS is_enrolled_in_vio
    , CASE WHEN f.creative_type = 'VAST' AND apps.video_viewthrough_enabled = TRUE THEN TRUE
           WHEN f.ad_format in ('banner','mrec') AND f.creative_type = 'HTML' AND apps.banner_viewthrough_enabled = TRUE THEN TRUE
           WHEN f.ad_format = 'interstitial' AND f.creative_type = 'HTML' AND apps.interstitial_viewthrough_enabled = TRUE THEN TRUE
           WHEN f.ad_format = 'native' AND apps.native_viewthrough_enabled = TRUE THEN TRUE  
           ELSE FALSE END AS viewthrough_enabled
    , sd.service_level
    , sd.sales_region AS campaign_sales_region
    , sd.sales_sub_region AS campaign_sales_sub_region
    , goals.type AS goal_type_3
    , goals.target_value AS goal_3_value
    , f.convx_percentile
    , f.convx_percentile_low
    , f.convx_percentile_high
    , f.preshaded_price_percentile
    , f.preshaded_price_percentile_low
    , f.preshaded_price_percentile_high
    , f.bid_target_percentile
    , f.bid_target_percentile_low
    , f.bid_target_percentile_high

    , sum(f.impressions) AS impressions
    , sum(f.clicks) AS clicks
    , sum(f.installs) AS installs
    , sum(f.internal_spend_micros) AS internal_spend_micros
    , sum(f.external_spend_micros) AS external_spend_micros
    , sum(IF(from_iso8601_timestamp(f.at) - from_iso8601_timestamp(IF(f.ad_group_type='reengagement', f.click_at, f.install_at)) < INTERVAL '7' DAY, f.customer_revenue_micros_d7, 0)) AS customer_revenue_micros_d7
    , sum(IF(from_iso8601_timestamp(f.at) - from_iso8601_timestamp(IF(f.ad_group_type='reengagement', f.click_at, f.install_at)) < INTERVAL '7' DAY, f.target_events_d7, 0)) AS target_events_d7
    , sum(IF(from_iso8601_timestamp(f.at) - from_iso8601_timestamp(IF(f.ad_group_type='reengagement', f.click_at, f.install_at)) < INTERVAL '7' DAY, f.target_events_first_d7, 0)) AS target_events_first_d7 
    , sum(predicted_conversion_likelihood) AS predicted_conversion_likelihood
    , sum(preshaded_cpm_micros) AS preshaded_cpm_micros
    , sum(private_cpm_micros) AS private_cpm_micros
    , sum(bid_target_total) AS bid_target_total
    , sum(effective_bid_target) AS effective_bid_target
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
  LEFT JOIN pinpoint.public.trackers trackers 
  	 ON c.tracker_id = trackers.id
  LEFT JOIN pinpoint.public.apps apps
     ON f.dest_app_id = apps.id
  LEFT JOIN pinpoint.public.ad_groups ag
     ON f.ad_group_id = ag.id    
  LEFT JOIN goals
     ON f.campaign_id = goals.campaign_id
  LEFT JOIN targets 
  	 ON f.campaign_id = targets.campaign_id
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43
