-- Per-run SQL quality scorecard, including all six named dimensions.
INSERT INTO dq_results
WITH checks AS (
  SELECT 'orders_required_customer' AS check_name, 'Completeness' AS dimension,
    'Critical' AS severity, COUNT(*) AS rows_checked,
    COUNT(*) FILTER (WHERE customer_id='') AS rows_failed FROM latest_orders
  UNION ALL SELECT 'orders_unique_key','Uniqueness','Critical',COUNT(*),
    COUNT(*)-COUNT(DISTINCT order_id) FROM raw_orders WHERE _run_id=(SELECT run_id FROM run_context)
  UNION ALL SELECT 'orders_valid_status','Validity','Critical',COUNT(*),
    COUNT(*) FILTER (WHERE order_status NOT IN ('DELIVERED','CANCELLED','RETURNED')) FROM stg_orders
  UNION ALL SELECT 'orders_amount_formula','Accuracy','Critical',COUNT(*),
    COUNT(*) FILTER (WHERE failure_reason LIKE '%order_amount_formula%') FROM stg_orders
  UNION ALL SELECT 'orders_known_refs','Consistency','Critical',COUNT(*),
    COUNT(*) FILTER (WHERE failure_reason LIKE '%known_%') FROM stg_orders
  UNION ALL SELECT 'orders_not_future','Timeliness','Critical',COUNT(*),
    COUNT(*) FILTER (WHERE failure_reason LIKE '%not_future_order%') FROM stg_orders
  UNION ALL SELECT 'orders_freshness','Timeliness','Warning',COUNT(*),
    CASE WHEN MAX(TRY_CAST(order_ts AS TIMESTAMP)) < (SELECT as_of_ts FROM run_context)-INTERVAL '1 day'
      THEN 1 ELSE 0 END FROM latest_orders
  UNION ALL SELECT 'orders_volume','Completeness','Warning',COUNT(*),
    CASE WHEN COUNT(*) < 1000 THEN 1 ELSE 0 END FROM latest_orders
  UNION ALL SELECT 'orders_price_distribution','Accuracy','Warning',COUNT(*),
    COUNT(*) FILTER (WHERE warning_reason LIKE '%price_distribution%') FROM stg_orders
  UNION ALL SELECT 'payments_known_order','Consistency','Critical',COUNT(*),
    COUNT(*) FILTER (WHERE failure_reason LIKE '%known_payment_order%') FROM stg_payments
  UNION ALL SELECT 'customers_unique_key','Uniqueness','Critical',COUNT(*),
    COUNT(*)-COUNT(DISTINCT customer_id) FROM raw_customers WHERE _run_id=(SELECT run_id FROM run_context)
  UNION ALL SELECT 'products_positive_mrp','Accuracy','Critical',COUNT(*),
    COUNT(*) FILTER (WHERE failure_reason LIKE '%positive_mrp%') FROM stg_products
  UNION ALL SELECT 'stores_zone_present','Completeness','Critical',COUNT(*),
    COUNT(*) FILTER (WHERE failure_reason LIKE '%store_zone_present%') FROM stg_stores
)
SELECT context.run_id, context.dag_id, check_name, dimension, severity,
  rows_checked, rows_failed, CASE WHEN rows_failed>0 THEN 'FAIL' ELSE 'PASS' END,
  CURRENT_TIMESTAMP
FROM checks CROSS JOIN run_context context;
