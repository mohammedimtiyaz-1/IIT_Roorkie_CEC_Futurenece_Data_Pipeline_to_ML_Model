-- Only staging rows that passed every critical rule reach business marts.
CREATE OR REPLACE TABLE mart_fact_orders AS
SELECT o.order_id, o.order_ts, o.order_date, o.customer_id, o.store_id,
  o.product_id, o.quantity, o.unit_price, o.discount_pct, o.order_amount,
  o.payment_mode, o.order_status, o.delivery_minutes,
  p.category, p.cost_price, s.store_name, s.zone, c.loyalty_tier,
  CASE WHEN o.order_status='DELIVERED' THEN o.order_amount ELSE 0 END AS net_revenue,
  CASE WHEN o.order_status='DELIVERED' THEN o.order_amount-o.quantity*p.cost_price ELSE 0 END AS gross_margin,
  o.order_status='DELIVERED' AND o.delivery_minutes<=15 AS within_15_min,
  o._source_file, o._source_row, o._run_id
FROM stg_orders o
JOIN stg_customers c ON o.customer_id=c.customer_id AND c.dq_status='Passed'
JOIN stg_products p ON o.product_id=p.product_id AND p.dq_status='Passed'
JOIN stg_stores s ON o.store_id=s.store_id AND s.dq_status='Passed'
WHERE o.dq_status='Passed';

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT order_date, zone, store_id, store_name,
  ROUND(SUM(net_revenue),2) AS net_revenue,
  COUNT(*) FILTER (WHERE order_status='DELIVERED') AS delivered_orders,
  ROUND(AVG(delivery_minutes) FILTER (WHERE order_status='DELIVERED'),2) AS avg_delivery_minutes,
  ROUND(100.0 * COUNT(*) FILTER (WHERE within_15_min)
      / NULLIF(COUNT(*) FILTER (WHERE order_status='DELIVERED'),0),2) AS within_15_pct
FROM mart_fact_orders GROUP BY 1,2,3,4;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT category, ROUND(SUM(net_revenue),2) AS net_revenue,
  ROUND(SUM(gross_margin),2) AS gross_margin
FROM mart_fact_orders GROUP BY 1;

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT store_id, store_name,
  ROUND(AVG(delivery_minutes) FILTER (WHERE order_status='DELIVERED'),2) AS avg_delivery_minutes,
  ROUND(100.0 * COUNT(*) FILTER (WHERE within_15_min)
      / NULLIF(COUNT(*) FILTER (WHERE order_status='DELIVERED'),0),2) AS within_15_pct,
  COUNT(*) FILTER (WHERE order_status='DELIVERED') AS delivered_orders
FROM mart_fact_orders GROUP BY 1,2;

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH successful AS (
  SELECT order_id, SUM(amount_paid) AS amount_paid
  FROM stg_payments WHERE dq_status='Passed' AND payment_status='SUCCESS' GROUP BY 1
)
SELECT f.order_id, f.order_amount, s.amount_paid,
  CASE WHEN s.order_id IS NULL THEN 'missing_success_payment'
       WHEN ABS(f.order_amount-s.amount_paid)>0.01 THEN 'amount_mismatch' END AS reason
FROM mart_fact_orders f LEFT JOIN successful s USING(order_id)
WHERE f.order_status='DELIVERED' AND
  (s.order_id IS NULL OR ABS(f.order_amount-s.amount_paid)>0.01)
UNION ALL
SELECT p.order_id, NULL, p.amount_paid, 'orphan_payment'
FROM stg_payments p LEFT JOIN mart_fact_orders f USING(order_id)
WHERE p.dq_status='Passed' AND f.order_id IS NULL;

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT f.customer_id, c.loyalty_tier, ROUND(SUM(f.net_revenue),2) AS spend,
  COUNT(*) FILTER (WHERE f.order_status='DELIVERED') AS delivered_orders
FROM mart_fact_orders f JOIN stg_customers c USING(customer_id)
GROUP BY 1,2 ORDER BY spend DESC LIMIT 10;

CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
SELECT payment_mode, ROUND(SUM(net_revenue),2) AS revenue
FROM mart_fact_orders GROUP BY 1;

CREATE OR REPLACE TABLE mart_rejections AS
SELECT file, TRIM(rule) AS failure_reason, COUNT(DISTINCT row_key) AS rejected_rows
FROM elt_quarantine, UNNEST(STRING_SPLIT(failure_reason, '; ')) AS split(rule)
WHERE TRIM(rule) <> ''
GROUP BY 1,2;

CREATE OR REPLACE TABLE mart_store_capacity AS
SELECT f.order_date, f.store_id, COUNT(*) AS order_count,
  s.capacity_orders_per_day,
  COUNT(*) > s.capacity_orders_per_day AS over_capacity
FROM mart_fact_orders f JOIN stg_stores s USING(store_id)
GROUP BY 1,2,4;
