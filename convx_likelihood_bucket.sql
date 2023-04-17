
  WITH price_percentile_split AS (
  -- 12 hours 3'37''
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
  WHERE date_diff('hour', from_iso8601_timestamp(dt), current_date) <= 12
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
, funnel AS (
    -- fetch impressions
    SELECT
    date_trunc('day', from_iso8601_timestamp(dt)) as dt
    , bid__app_platform AS platform
    , bid__bid_request__exchange AS exchange
    , CASE WHEN bid__creative__ad_format = 'video' THEN 'VAST'
           WHEN bid__creative__ad_format = 'native' THEN 'native'
           WHEN bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
           WHEN  bid__creative__ad_format = '300x250' THEN  'mrec'
           ELSE 'html-interstitial' END AS ad_format
    , bid__price_data__model_type AS model_type
    , bid__customer_id AS customer_id
    , bid__app_id AS dest_app_id
    , bid__campaign_id AS campaign_id
    , bid__ad_group_id AS ad_group_id
    , bid__ad_group_type AS ad_group_type
    , bid__creative__type AS creative_type
    , sum(spend_micros) AS internal_spend_micros
    , sum(revenue_micros) AS external_spend_micros
    , sum(bid__price_data__conversion_likelihood) AS predicted_conversion_likelihood
    FROM rtb.impressions_with_bids a
    WHERE dt BETWEEN '2023-04-16' AND '2023-04-19'
    	AND bid__customer_id = 753
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
 )
    SELECT
    f.dt
    , f.customer_id
    , f.dest_app_id
    , f.campaign_id
    , f.ad_group_id
    , f.ad_group_type
    , f.creative_type
    , f.ad_format
    , f.platform
    , f.exchange
    , f.model_type
    , cb.name AS convx_percentile
    , sum(f.internal_spend_micros) AS internal_spend_micros
    , sum(f.external_spend_micros) AS external_spend_micros
    , sum(predicted_conversion_likelihood) AS predicted_conversion_likelihood
  FROM funnel f
  LEFT JOIN convx_buckets cb
     ON f.platform = cb.platform
        AND f.model_type = cb.model_type
        AND f.ad_format = cb.ad_format
        AND f.exchange = cb.exchange
        AND f.predicted_conversion_likelihood >= cb.low 
        AND (cb.high IS NULL OR f.predicted_conversion_likelihood < cb.high)
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12 
