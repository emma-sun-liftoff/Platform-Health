SELECT r.time_bucket as "time"
    , SUM(h.revenue_micros)::float / 1000000 as revenue
    , SUM(CASE WHEN r.predicted_revenue_micros::float IN ('NaN', 'Infinity', '-Infinity') THEN NULL ELSE r.predicted_revenue_micros END)::float / 1000000 AS predicted_revenue
    , SUM(r.suggested_revenue_micros)::float / 1000000 AS suggested_revenue
    , SUM(r.planned_revenue_micros)::float / 1000000 AS planned_revenue
FROM pacer.ad_group_results r
LEFT JOIN ad_group_history h 
  ON r.time_bucket = h.time_bucket 
  AND r.ab_test_group_ids = h.ab_test_group_ids 
  AND r.ad_group_id = h.ad_group_id
WHERE r.time_bucket > now() - interval <Parameters.Days of Data (Pacer Revenue Calibration)>
  AND h.time_bucket > now() - interval <Parameters.Days of Data (Pacer Revenue Calibration)>
  AND r.time_bucket < now() - interval '1 hour'
  AND h.time_bucket < now() - interval '1 hour'
  AND h.enabled = TRUE
  AND bid_target_micros > 0
  AND h.estimated = TRUE
  AND r.suggested_bid_target_micros IS NOT NULL
GROUP BY 1
