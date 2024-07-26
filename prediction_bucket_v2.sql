CREATE TABLE IF NOT EXISTS prediction_bucket_v2 AS 
WITH convx_percentile_split AS (
 SELECT
 	bid__app_platform AS platform
    , CASE WHEN bid__price_data__model_type IN ('revenue','revenue-v3') THEN 'revenue'
    	   ELSE bid__price_data__model_type END AS model_type
    , approx_percentile(bid__price_data__conversion_likelihood,
        ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]) AS convx_likelihood_percentile
  FROM rtb.impressions_with_bids
  WHERE dt >='2024-06-13T00' AND dt < '2024-06-27T00'
    AND CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') >= '2024-06-13T00'
    AND CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') < '2024-06-27T00'
    AND bid__price_data__model_type != ''
    AND bid__app_platform IN ('ANDROID', 'IOS')
  GROUP BY 1,2
  )
  , private_cpm_percentile_split AS ( 
 SELECT
 	bid__app_platform AS platform
    , CASE WHEN bid__price_data__model_type IN ('revenue','revenue-v3') THEN 'revenue'
    	   ELSE bid__price_data__model_type END AS model_type
    , approx_percentile(CAST(bid__auction_result__winner__price_cpm_micros AS double)/bid__price_data__compensated_margin_bid_multiplier/1000000,
        ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]) AS private_cpm_percentile
  FROM rtb.impressions_with_bids
  WHERE dt >='2024-06-13T00' AND dt < '2024-06-27T00'
    AND CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') >= '2024-06-13T00'
    AND CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(at/1000, 'UTC'))),1,19),'Z') < '2024-06-27T00'
    AND bid__price_data__model_type != ''
    AND bid__app_platform IN ('ANDROID', 'IOS')
  GROUP BY 1,2
  )
  , convx_percentile_bucket AS (
  SELECT
  platform
  , model_type
  , CAST(
        zip(
         ARRAY[0] || convx_likelihood_percentile, convx_likelihood_percentile || CAST(ARRAY[null] as ARRAY(DOUBLE)),
         ARRAY['0-10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', 
	 '70-80', '80-90', '90-95', '95-99', '99-100']
        ) AS ARRAY(ROW(low DOUBLE, high DOUBLE, name VARCHAR))) AS convx_likelihood_bucket
 FROM convx_percentile_split
 )
  , private_cpm_percentile_bucket AS (
  SELECT
  platform
  , model_type
  , CAST(
        zip(
         ARRAY[0] || private_cpm_percentile, private_cpm_percentile || CAST(ARRAY[null] as ARRAY(DOUBLE)),
         ARRAY['0-10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', 
	 '70-80', '80-90', '90-95', '95-99', '99-100']
        ) AS ARRAY(ROW(low DOUBLE, high DOUBLE, name VARCHAR))) AS private_cpm_bucket
 FROM private_cpm_percentile_split
 )

  SELECT 
  cv.platform
  , cv.model_type
  , clb.low AS convx_percentile_low
  , clb.high AS convx_percentile_high
  , clb.name AS convx_percentile
  , ppb.low AS private_cpm_percentile_low
  , ppb.high AS private_cpm_percentile_high
  , ppb.name AS private_cpm_percentile
  FROM convx_percentile_bucket cv
  JOIN private_cpm_percentile_bucket p
  	ON cv.platform = p.platform
  	AND cv.model_type = p.model_type
  CROSS JOIN UNNEST(convx_likelihood_bucket) AS clb
  CROSS JOIN UNNEST(private_cpm_bucket) AS ppb
 
