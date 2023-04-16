SELECT
  impression_at
  , platform
  , exchange
  , model_type
  , ad_group_id
  , ad_format
  , CAST(
    	zip(
     	 ARRAY[0] || price_percentile, price_percentile || cast(ARRAY[null] as ARRAY(DOUBLE)),
      	 ARRAY['0-10', '10-20', '20-30', '30-40', '40-50', '50-60', '60-70', '70-80', '80-90', '90-95', '95-99', '99-99.99', '99.99-100']
   		) AS ARRAY(ROW(low DOUBLE, high DOUBLE, name VARCHAR))) AS percentiles
FROM (
  SELECT
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') AS impression_at
    , bid__app_platform AS platform
    , bid__bid_request__exchange AS exchange
    , bid__price_data__model_type AS model_type
    , bid__ad_group_id AS ad_group_id
	, CASE WHEN bid__creative__ad_format = 'video' THEN 'VAST'
	   	   WHEN bid__creative__ad_format = 'native' THEN 'native'
	       WHEN bid__creative__ad_format IN ('320x50', '728x90') THEN 'banner'
	       WHEN  bid__creative__ad_format = '300x250' THEN  'mrec'
	       ELSE 'html-interstitial' END AS ad_format
    , approx_percentile(bid__price_data__conversion_likelihood,
        ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.9999]) AS price_percentile
  FROM rtb.impressions_with_bids
  WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
    AND bid__price_data__model_type != ''
  GROUP BY 1,2,3,4,5,6
)
