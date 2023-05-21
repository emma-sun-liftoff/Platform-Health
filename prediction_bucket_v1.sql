CREATE TABLE IF NOT EXISTS prediction_bucket_v1 AS 
 WITH convx_percentile_split AS (
 SELECT
 	bid__app_platform AS platform
    , CASE WHEN bid__bid_request__exchange IN ('VUNGLE',
		'APPLOVIN',
		'INNERACTIVE_DIRECT',
		'DOUBLECLICK',
		'MINTEGRAL',
		'IRONSOURCE',
		'UNITY',
		'APPODEAL',
		'INMOBI',
		'VERVE') THEN bid__bid_request__exchange ELSE 'others' END AS exchange_group
    , CASE WHEN bid__price_data__model_type IN ('revenue','revenue-v3') THEN 'revenue'
    	   ELSE bid__price_data__model_type END AS model_type
    , CASE WHEN bid__creative__ad_format = 'video' THEN 'VAST' ELSE 'Non-VAST' END AS ad_format_group
    , approx_percentile(bid__price_data__conversion_likelihood,
        ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]) AS covx_likelihood_percentile
  FROM rtb.impressions_with_bids
  WHERE dt > '2023-05-09T00' AND dt < '2023-05-16T00'
    AND CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') > '2023-05-09T00'
    AND CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') < '2023-05-16T00'
    AND bid__price_data__model_type != ''
  GROUP BY 1,2,3,4
  )
  , private_cpm_percentile_split AS ( 
 SELECT
 	bid__app_platform AS platform
    , CASE WHEN bid__bid_request__exchange IN ('VUNGLE',
		'APPLOVIN',
		'INNERACTIVE_DIRECT',
		'DOUBLECLICK',
		'MINTEGRAL',
		'IRONSOURCE',
		'UNITY',
		'APPODEAL',
		'INMOBI',
		'VERVE') THEN bid__bid_request__exchange ELSE 'others' END AS exchange_group
    , CASE WHEN bid__price_data__model_type IN ('revenue','revenue-v3') THEN 'revenue'
    	   ELSE bid__price_data__model_type END AS model_type
    , CASE WHEN bid__creative__ad_format = 'video' THEN 'VAST' ELSE 'Non-VAST' END AS ad_format_group
    , approx_percentile(CAST(bid__auction_result__winner__price_cpm_micros AS double)/bid__price_data__compensated_margin_bid_multiplier/1000000,
        ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]) AS private_cpm_percentile
  FROM rtb.impressions_with_bids
  WHERE dt > '2023-05-09T00' AND dt < '2023-05-16T00'
    AND CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') > '2023-05-09T00'
    AND CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') < '2023-05-16T00'
    AND bid__price_data__model_type != ''
  GROUP BY 1,2,3,4
  )
  , cpm_percentile_split AS (
 SELECT
 	bid__app_platform AS platform
    , CASE WHEN bid__bid_request__exchange IN ('VUNGLE',
		'APPLOVIN',
		'INNERACTIVE_DIRECT',
		'DOUBLECLICK',
		'MINTEGRAL',
		'IRONSOURCE',
		'UNITY',
		'APPODEAL',
		'INMOBI',
		'VERVE') THEN bid__bid_request__exchange ELSE 'others' END AS exchange_group
    , CASE WHEN bid__price_data__model_type IN ('revenue','revenue-v3') THEN 'revenue'
    	   ELSE bid__price_data__model_type END AS model_type
    , CASE WHEN bid__creative__ad_format = 'video' THEN 'VAST' ELSE 'Non-VAST' END AS ad_format_group
    , approx_percentile(CAST(bid__price_cpm_micros AS double)/1000000,
        ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]) AS cpm_percentile
  FROM rtb.impressions_with_bids
  WHERE dt > '2023-05-09T00' AND dt < '2023-05-16T00'
    AND CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') > '2023-05-09T00'
    AND CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') < '2023-05-16T00'
    AND bid__price_data__model_type != ''
  GROUP BY 1,2,3,4
  )
  , convx_percentile_bucket AS (
  SELECT
  platform
  , model_type
  , exchange_group
  , ad_format_group
  , CAST(
        zip(
         ARRAY[0] || covx_likelihood_percentile, covx_likelihood_percentile || CAST(ARRAY[null] as ARRAY(DOUBLE)),
         ARRAY['0-10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', 
	 '70-80', '80-90', '90-95', '95-99', '99-100']
        ) AS ARRAY(ROW(low DOUBLE, high DOUBLE, name VARCHAR))) AS convx_likelihood_bucket
 FROM convx_percentile_split
 )
  , private_cpm_percentile_bucket AS (
  SELECT
  platform
  , model_type
  , exchange_group
  , ad_format_group
  , CAST(
        zip(
         ARRAY[0] || private_cpm_percentile, private_cpm_percentile || CAST(ARRAY[null] as ARRAY(DOUBLE)),
         ARRAY['0-10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', 
	 '70-80', '80-90', '90-95', '95-99', '99-100']
        ) AS ARRAY(ROW(low DOUBLE, high DOUBLE, name VARCHAR))) AS private_cpm_bucket
 FROM private_cpm_percentile_split
 )
  , cpm_percentile_bucket AS (
  SELECT
  platform
  , model_type
  , exchange_group
  , ad_format_group
  , CAST(
        zip(
         ARRAY[0] || cpm_percentile, cpm_percentile || CAST(ARRAY[null] as ARRAY(DOUBLE)),
         ARRAY['0-10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', 
	 '70-80', '80-90', '90-95', '95-99', '99-100']
        ) AS ARRAY(ROW(low DOUBLE, high DOUBLE, name VARCHAR))) AS cpm_bucket
 FROM cpm_percentile_split
 )

  SELECT 
  cv.platform
  , cv.model_type
  , cv.exchange_group
  , cv.ad_format_group
  , clb.low AS convx_percentile_low
  , clb.high AS convx_percentile_high
  , clb.name AS convx_percentile
  , ppb.low AS private_cpm_percentile_low
  , ppb.high AS private_cpm_percentile_high
  , ppb.name AS private_cpm_percentile
  , btb.low AS cpm_percentile_low
  , btb.high AS cpm_percentile_high
  , btb.name AS cpm_percentile
  FROM convx_percentile_bucket cv
  JOIN private_cpm_percentile_bucket p
  	ON cv.platform = p.platform
  	AND cv.model_type = p.model_type
  	AND cv.exchange_group = p.exchange_group
  	AND cv.ad_format_group = p.ad_format_group
  JOIN cpm_percentile_bucket c 
  	ON cv.platform = c.platform
  	AND cv.model_type = c.model_type
  	AND cv.exchange_group = c.exchange_group
  	AND cv.ad_format_group = c.ad_format_group  
  CROSS JOIN UNNEST(convx_likelihood_bucket) AS clb
  CROSS JOIN UNNEST(private_cpm_bucket) AS ppb
  CROSS JOIN UNNEST(cpm_bucket) AS btb
 
