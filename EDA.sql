-- Data Validation Query
SELECT 
 bid__ad_group_id
, CAST(CASE WHEN bid_type IN ('cpr', 'cprv3') THEN bid__price_data__conversion_likelihood/1000000 ELSE bid__price_data__conversion_likelihood END AS double) AS p_cvr
, CAST(bid__price_data__ad_group_cpx_bid_micros AS double)/1000000  as bid_target
, CAST(CASE WHEN bid_type IN ('cpr', 'cprv3') THEN bid__price_data__conversion_likelihood/1000000 ELSE bid__price_data__conversion_likelihood END AS double) * bid__price_data__effective_cpx_bid_micros/1000000 AS preshaded_w_multiplier
, CAST(bid__auction_result__winner__price_cpm_micros AS double)/1000000000 AS preshaded_value 
, CAST(bid__auction_result__winner__price_cpm_micros AS double)/1000000000/CAST(bid__price_data__compensated_margin_bid_multiplier AS double) AS private_value
, CAST(bid__price_cpm_micros AS double)/1000000000 AS CPM_per_impression 
, CAST(revenue_micros AS double)/1000000 AS revenue
, CAST(spend_micros AS double)/1000000 AS spend
, 1 - CAST(spend_micros AS double)/CAST(revenue_micros AS double) AS nrm_by_cal
, CAST(bid__price_data__compensated_margin_bid_multiplier AS double) AS margin_multiplier
, bid__price_data__effective_cpx_bid_micros AS bid_target_with_multiplier
FROM rtb.impressions_with_bids f
LEFT JOIN pinpoint.public.ad_groups ag
     ON f.bid__ad_group_id = ag.id 
WHERE dt between '2023-05-07T01' AND '2023-05-07T02'
-- AND ag.bid_type = 'cpa'
--AND ag.viewthrough_optimization_enabled <> FALSE
AND f.bid__ad_group_id IN (111460, 162802)
LIMIT 5 
