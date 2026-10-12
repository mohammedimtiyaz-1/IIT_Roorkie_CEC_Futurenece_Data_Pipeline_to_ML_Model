-- ELT transformations are SQL-only. Raw input remains unchanged and append-only.
CREATE OR REPLACE TEMP TABLE latest_customers AS
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC, _source_row ASC) AS rn
  FROM raw_customers
)
SELECT * FROM ranked WHERE rn = 1;

CREATE OR REPLACE TABLE stg_customers AS
WITH cleaned AS (
  SELECT customer_id, city, UPPER(TRIM(loyalty_tier)) AS loyalty_tier,
    CASE WHEN regexp_matches(email, '^[^@ ]+@[^@ ]+\\.[^@ ]+$')
      THEN LEFT(email, 2) || '***@' || split_part(email, '@', 2) ELSE '[invalid]' END AS masked_email,
    CASE WHEN regexp_matches(phone, '^[0-9]{10}$')
      THEN LEFT(phone, 2) || '******' || RIGHT(phone, 2) ELSE '[invalid]' END AS masked_phone,
    _source_file, _source_row, _run_id,
    concat_ws('; ',
      CASE WHEN TRIM(customer_id) = '' THEN 'required_key' END,
      CASE WHEN TRY_CAST(signup_date AS DATE) IS NULL THEN 'valid_signup_date' END,
      CASE WHEN TRY_CAST(signup_date AS DATE) > CURRENT_DATE THEN 'not_future_signup' END
    ) AS failure_reason,
    concat_ws('; ',
      CASE WHEN NOT regexp_matches(email, '^[^@ ]+@[^@ ]+\\.[^@ ]+$') THEN 'valid_email' END,
      CASE WHEN NOT regexp_matches(phone, '^[0-9]{10}$') THEN 'valid_phone' END,
      CASE WHEN NOT regexp_matches(pincode, '^[0-9]{6}$') THEN 'valid_customer_pincode' END,
      CASE WHEN TRIM(city) = '' THEN 'customer_city_present' END
    ) AS warning_reason
  FROM latest_customers
)
SELECT *, CASE WHEN failure_reason = '' THEN 'Passed' ELSE 'Critical' END AS dq_status FROM cleaned;

CREATE OR REPLACE TEMP TABLE latest_products AS
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _loaded_at DESC, _source_row ASC) AS rn
  FROM raw_products
)
SELECT * FROM ranked WHERE rn = 1;

CREATE OR REPLACE TABLE stg_products AS
WITH cleaned AS (
  SELECT product_id, product_name, category, brand,
    TRY_CAST(mrp AS DOUBLE) AS mrp, TRY_CAST(cost_price AS DOUBLE) AS cost_price,
    _source_file, _source_row, _run_id,
    concat_ws('; ',
      CASE WHEN TRIM(product_id) = '' THEN 'required_key' END,
      CASE WHEN TRIM(category) = '' THEN 'product_category_present' END,
      CASE WHEN TRY_CAST(mrp AS DOUBLE) IS NULL OR TRY_CAST(mrp AS DOUBLE) <= 0 THEN 'positive_mrp' END,
      CASE WHEN TRY_CAST(cost_price AS DOUBLE) IS NULL OR TRY_CAST(cost_price AS DOUBLE) < 0
                  OR TRY_CAST(cost_price AS DOUBLE) > TRY_CAST(mrp AS DOUBLE) THEN 'valid_cost' END,
      CASE WHEN TRY_CAST(launch_date AS DATE) IS NULL THEN 'valid_launch_date' END,
      CASE WHEN TRY_CAST(launch_date AS DATE) > CURRENT_DATE THEN 'not_future_launch' END
    ) AS failure_reason
  FROM latest_products
)
SELECT *, CASE WHEN failure_reason = '' THEN 'Passed' ELSE 'Critical' END AS dq_status FROM cleaned;

CREATE OR REPLACE TEMP TABLE latest_stores AS
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY _loaded_at DESC, _source_row ASC) AS rn
  FROM raw_stores
)
SELECT * FROM ranked WHERE rn = 1;

CREATE OR REPLACE TABLE stg_stores AS
WITH cleaned AS (
  SELECT store_id, store_name, zone,
    TRY_CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
    _source_file, _source_row, _run_id,
    concat_ws('; ',
      CASE WHEN TRIM(store_id) = '' THEN 'required_key' END,
      CASE WHEN TRIM(zone) = '' THEN 'store_zone_present' END,
      CASE WHEN TRY_CAST(capacity_orders_per_day AS INTEGER) IS NULL
                  OR TRY_CAST(capacity_orders_per_day AS INTEGER) <= 0 THEN 'valid_store_capacity' END,
      CASE WHEN TRY_CAST(open_date AS DATE) > CURRENT_DATE THEN 'not_future_opening' END
    ) AS failure_reason,
    CASE WHEN NOT regexp_matches(pincode, '^[0-9]{6}$') THEN 'valid_store_pincode' ELSE '' END AS warning_reason
  FROM latest_stores
)
SELECT *, CASE WHEN failure_reason = '' THEN 'Passed' ELSE 'Critical' END AS dq_status FROM cleaned;

CREATE OR REPLACE TEMP TABLE latest_orders AS
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY order_id ORDER BY
      CASE WHEN _source_file LIKE '%increment%' THEN 1 ELSE 0 END DESC,
      _loaded_at DESC, _source_row ASC) AS rn
  FROM raw_orders
)
SELECT * FROM ranked WHERE rn = 1;

CREATE OR REPLACE TEMP TABLE order_candidates AS
WITH casted AS (
  SELECT o.order_id, o.order_ts, o.customer_id, o.store_id, o.product_id,
    TRY_CAST(o.quantity AS DOUBLE) AS quantity, TRY_CAST(o.unit_price AS DOUBLE) AS unit_price,
    TRY_CAST(o.discount_pct AS DOUBLE) AS discount_pct,
    TRY_CAST(o.order_amount AS DOUBLE) AS order_amount,
    CASE UPPER(TRIM(o.payment_mode))
      WHEN 'UPI' THEN 'UPI' WHEN 'CARD' THEN 'Card'
      WHEN 'COD' THEN 'Cash on Delivery' WHEN 'CASH ON DELIVERY' THEN 'Cash on Delivery'
      WHEN 'WALLET' THEN 'Wallet' ELSE TRIM(o.payment_mode) END AS payment_mode,
    UPPER(TRIM(o.order_status)) AS order_status,
    TRY_CAST(o.delivery_minutes AS DOUBLE) AS delivery_minutes,
    TRY_CAST(o.order_ts AS TIMESTAMP) AS parsed_order_ts,
    o._source_file, o._source_row, o._run_id,
    c.customer_id AS valid_customer_id, p.product_id AS valid_product_id,
    s.store_id AS valid_store_id, p.mrp AS product_mrp
  FROM latest_orders o
  LEFT JOIN stg_customers c ON o.customer_id=c.customer_id AND c.dq_status='Passed'
  LEFT JOIN stg_products p ON o.product_id=p.product_id AND p.dq_status='Passed'
  LEFT JOIN stg_stores s ON o.store_id=s.store_id AND s.dq_status='Passed'
), checked AS (
  SELECT order_id, order_ts, CAST(parsed_order_ts AS DATE) AS order_date,
    customer_id, store_id, product_id, quantity, unit_price, discount_pct,
    order_amount, payment_mode, order_status, delivery_minutes,
    _source_file, _source_row, _run_id,
    concat_ws('; ',
      CASE WHEN TRIM(order_id)='' THEN 'required_key' END,
      CASE WHEN TRIM(customer_id)='' THEN 'required_customer' END,
      CASE WHEN parsed_order_ts IS NULL THEN 'valid_order_timestamp' END,
      CASE WHEN parsed_order_ts > CURRENT_TIMESTAMP THEN 'not_future_order' END,
      CASE WHEN order_status NOT IN ('DELIVERED','CANCELLED','RETURNED') THEN 'valid_order_status' END,
      CASE WHEN payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') THEN 'valid_order_mode' END,
      CASE WHEN quantity IS NULL OR quantity<=0 OR unit_price IS NULL OR unit_price<=0
        THEN 'positive_quantity_price' END,
      CASE WHEN discount_pct IS NULL OR discount_pct<0 OR discount_pct>100 THEN 'valid_discount' END,
      CASE WHEN order_amount IS NULL OR quantity IS NULL OR unit_price IS NULL OR discount_pct IS NULL
              OR ABS(ROUND(quantity*unit_price*(1-discount_pct/100),2)-order_amount)>0.01
        THEN 'order_amount_formula' END,
      CASE WHEN order_status='DELIVERED' AND delivery_minutes IS NULL THEN 'delivered_has_minutes' END,
      CASE WHEN delivery_minutes IS NOT NULL AND (delivery_minutes<=0 OR delivery_minutes>=120)
        THEN 'plausible_delivery' END,
      CASE WHEN valid_customer_id IS NULL THEN 'known_customer_id' END,
      CASE WHEN valid_product_id IS NULL THEN 'known_product_id' END,
      CASE WHEN valid_store_id IS NULL THEN 'known_store_id' END
    ) AS failure_reason,
    CASE WHEN product_mrp IS NOT NULL AND unit_price>10*product_mrp
      THEN 'price_distribution' ELSE '' END AS warning_reason
  FROM casted
)
SELECT *, CASE WHEN failure_reason='' THEN 'Passed' ELSE 'Critical' END AS dq_status FROM checked;

CREATE TABLE IF NOT EXISTS stg_orders AS SELECT * FROM order_candidates WHERE FALSE;
MERGE INTO stg_orders AS target USING order_candidates AS source
ON target.order_id=source.order_id
WHEN MATCHED THEN UPDATE SET
  order_ts=source.order_ts, order_date=source.order_date,
  customer_id=source.customer_id, store_id=source.store_id, product_id=source.product_id,
  quantity=source.quantity, unit_price=source.unit_price, discount_pct=source.discount_pct,
  order_amount=source.order_amount, payment_mode=source.payment_mode,
  order_status=source.order_status, delivery_minutes=source.delivery_minutes,
  _source_file=source._source_file, _source_row=source._source_row, _run_id=source._run_id,
  failure_reason=source.failure_reason, warning_reason=source.warning_reason,
  dq_status=source.dq_status
WHEN NOT MATCHED THEN INSERT BY NAME;

CREATE OR REPLACE TEMP TABLE latest_payments AS
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _loaded_at DESC, _source_row ASC) AS rn
  FROM raw_payments
)
SELECT * FROM ranked WHERE rn=1;

CREATE OR REPLACE TABLE stg_payments AS
WITH casted AS (
  SELECT p.payment_id, p.order_id, p.payment_ts,
    CASE UPPER(TRIM(p.payment_mode))
      WHEN 'UPI' THEN 'UPI' WHEN 'CARD' THEN 'Card'
      WHEN 'COD' THEN 'Cash on Delivery' WHEN 'CASH ON DELIVERY' THEN 'Cash on Delivery'
      WHEN 'WALLET' THEN 'Wallet' ELSE TRIM(p.payment_mode) END AS payment_mode,
    TRY_CAST(p.amount_paid AS DOUBLE) AS amount_paid,
    UPPER(TRIM(p.payment_status)) AS payment_status,
    p._source_file, p._source_row, p._run_id,
    o.order_id AS valid_order_id
  FROM latest_payments p LEFT JOIN stg_orders o
    ON p.order_id=o.order_id AND o.dq_status='Passed'
), checked AS (
  SELECT payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status,
    _source_file, _source_row, _run_id,
    concat_ws('; ',
      CASE WHEN TRIM(payment_id)='' THEN 'required_key' END,
      CASE WHEN TRIM(order_id)='' THEN 'required_order_reference' END,
      CASE WHEN valid_order_id IS NULL THEN 'known_payment_order' END,
      CASE WHEN TRY_CAST(payment_ts AS TIMESTAMP) IS NULL THEN 'valid_payment_timestamp' END,
      CASE WHEN TRY_CAST(payment_ts AS TIMESTAMP)>CURRENT_TIMESTAMP THEN 'not_future_payment' END,
      CASE WHEN payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') THEN 'valid_payment_mode' END,
      CASE WHEN payment_status NOT IN ('SUCCESS','FAILED','REFUNDED') THEN 'valid_payment_status' END,
      CASE WHEN payment_status='SUCCESS' AND (amount_paid IS NULL OR amount_paid<0)
        THEN 'successful_payment_nonnegative' END
    ) AS failure_reason
  FROM casted
)
SELECT *, CASE WHEN failure_reason='' THEN 'Passed' ELSE 'Critical' END AS dq_status FROM checked;

CREATE OR REPLACE TABLE elt_quarantine AS
SELECT 'customers.csv' AS file, customer_id AS row_key, _source_row, failure_reason
  FROM stg_customers WHERE dq_status='Critical'
UNION ALL SELECT 'products.csv', product_id, _source_row, failure_reason
  FROM stg_products WHERE dq_status='Critical'
UNION ALL SELECT 'stores.csv', store_id, _source_row, failure_reason
  FROM stg_stores WHERE dq_status='Critical'
UNION ALL SELECT _source_file, order_id, _source_row, failure_reason
  FROM stg_orders WHERE dq_status='Critical'
UNION ALL SELECT 'payments.csv', payment_id, _source_row, failure_reason
  FROM stg_payments WHERE dq_status='Critical'
UNION ALL SELECT _source_file, order_id, _source_row, 'unique_key'
  FROM raw_orders r WHERE r._run_id=(SELECT run_id FROM run_context) AND EXISTS (
    SELECT 1 FROM raw_orders earlier
    WHERE earlier.order_id=r.order_id AND earlier._run_id=r._run_id
      AND earlier._source_file=r._source_file AND earlier._source_row<r._source_row)
UNION ALL SELECT _source_file, customer_id, _source_row, 'unique_key'
  FROM raw_customers r WHERE r._run_id=(SELECT run_id FROM run_context) AND EXISTS (
    SELECT 1 FROM raw_customers earlier
    WHERE earlier.customer_id=r.customer_id AND earlier._run_id=r._run_id
      AND earlier._source_row<r._source_row)
UNION ALL SELECT _source_file, product_id, _source_row, 'unique_key'
  FROM raw_products r WHERE r._run_id=(SELECT run_id FROM run_context) AND EXISTS (
    SELECT 1 FROM raw_products earlier
    WHERE earlier.product_id=r.product_id AND earlier._run_id=r._run_id
      AND earlier._source_row<r._source_row)
UNION ALL SELECT _source_file, payment_id, _source_row, 'unique_key'
  FROM raw_payments r WHERE r._run_id=(SELECT run_id FROM run_context) AND EXISTS (
    SELECT 1 FROM raw_payments earlier
    WHERE earlier.payment_id=r.payment_id AND earlier._run_id=r._run_id
      AND earlier._source_row<r._source_row);
