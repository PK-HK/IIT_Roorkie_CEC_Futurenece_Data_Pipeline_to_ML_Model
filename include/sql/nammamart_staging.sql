CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id VARCHAR,
    masked_email VARCHAR,
    masked_phone VARCHAR,
    loyalty_tier VARCHAR,
    is_active VARCHAR,
    dq_status VARCHAR,
    failure_reason VARCHAR,
    warning_reason VARCHAR,
    _loaded_at TIMESTAMPTZ,
    _source_file VARCHAR,
    _run_id VARCHAR
);

CREATE OR REPLACE TEMP TABLE latest_customers AS
WITH current_source AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY customer_id, _run_id) AS key_count,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id ORDER BY _loaded_at DESC, _run_id DESC
           ) AS latest_row
    FROM raw_customers
)
SELECT customer_id, full_name, email, phone, city, area, pincode,
       TRY_CAST(signup_date AS DATE) AS signup_date,
       UPPER(TRIM(loyalty_tier)) AS loyalty_tier,
       loyalty_tier AS raw_loyalty_tier,
       UPPER(TRIM(is_active)) AS is_active,
       _loaded_at, _source_file, _run_id, key_count
FROM current_source
WHERE latest_row = 1;

CREATE OR REPLACE TABLE stg_customers AS
SELECT customer_id,
       CASE WHEN REGEXP_FULL_MATCH(TRIM(email), '[^@[:space:]]+@[^@[:space:]]+[.][^@[:space:]]+')
            THEN LEFT(TRIM(email), 2) || '***@' || SPLIT_PART(TRIM(email), '@', 2)
            ELSE '***' END AS masked_email,
       CASE WHEN REGEXP_FULL_MATCH(TRIM(phone), '[0-9]{10}')
            THEN LEFT(TRIM(phone), 2) || '******' || RIGHT(TRIM(phone), 2)
            ELSE '***' END AS masked_phone,
       loyalty_tier, is_active,
       CASE
         WHEN TRIM(COALESCE(customer_id, '')) = ''
           OR TRIM(COALESCE(full_name, '')) = ''
           OR signup_date IS NULL OR signup_date > CURRENT_DATE
           OR key_count > 1 THEN 'FAIL'
         WHEN NOT REGEXP_FULL_MATCH(TRIM(email), '[^@[:space:]]+@[^@[:space:]]+[.][^@[:space:]]+')
           OR NOT REGEXP_FULL_MATCH(TRIM(phone), '[0-9]{10}')
           OR NOT REGEXP_FULL_MATCH(TRIM(pincode), '[0-9]{6}')
           OR TRIM(COALESCE(raw_loyalty_tier, '')) <> UPPER(TRIM(COALESCE(raw_loyalty_tier, '')))
           THEN 'WARN'
         ELSE 'PASS'
       END AS dq_status,
       CONCAT_WS('; ',
         CASE WHEN TRIM(COALESCE(customer_id, '')) = '' OR TRIM(COALESCE(full_name, '')) = ''
              THEN 'missing customer_id or full_name' END,
         CASE WHEN signup_date IS NULL THEN 'invalid signup_date' END,
         CASE WHEN signup_date > CURRENT_DATE THEN 'future signup_date' END,
         CASE WHEN key_count > 1 THEN 'duplicate customer_id' END
       ) AS failure_reason,
       CONCAT_WS('; ',
         CASE WHEN NOT REGEXP_FULL_MATCH(TRIM(email), '[^@[:space:]]+@[^@[:space:]]+[.][^@[:space:]]+')
              THEN 'invalid email' END,
         CASE WHEN NOT REGEXP_FULL_MATCH(TRIM(phone), '[0-9]{10}')
              THEN 'invalid phone' END,
         CASE WHEN NOT REGEXP_FULL_MATCH(TRIM(pincode), '[0-9]{6}')
              THEN 'invalid pincode' END,
         CASE WHEN TRIM(COALESCE(raw_loyalty_tier, '')) <> UPPER(TRIM(COALESCE(raw_loyalty_tier, '')))
              THEN 'loyalty tier casing standardised' END
       ) AS warning_reason,
       _loaded_at, _source_file, _run_id
FROM latest_customers;

CREATE TABLE IF NOT EXISTS stg_products (
    product_id VARCHAR,
    product_name VARCHAR,
    category VARCHAR,
    sub_category VARCHAR,
    brand VARCHAR,
    mrp DOUBLE,
    cost_price DOUBLE,
    unit VARCHAR,
    is_perishable VARCHAR,
    launch_date DATE,
    dq_status VARCHAR,
    failure_reason VARCHAR,
    warning_reason VARCHAR,
    _loaded_at TIMESTAMPTZ,
    _source_file VARCHAR,
    _run_id VARCHAR
);

CREATE OR REPLACE TEMP TABLE latest_products AS
WITH current_source AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY product_id, _run_id) AS key_count,
           ROW_NUMBER() OVER (
               PARTITION BY product_id ORDER BY _loaded_at DESC, _run_id DESC
           ) AS latest_row
    FROM raw_products
)
SELECT *, TRY_CAST(mrp AS DOUBLE) AS mrp_number,
       TRY_CAST(cost_price AS DOUBLE) AS cost_number,
       TRY_CAST(launch_date AS DATE) AS launch_day
FROM current_source
WHERE latest_row = 1;

CREATE OR REPLACE TABLE stg_products AS
SELECT product_id, product_name, NULLIF(TRIM(category), '') AS category,
       sub_category, brand, mrp_number AS mrp, cost_number AS cost_price,
       unit, is_perishable, launch_day AS launch_date,
       CASE WHEN TRIM(COALESCE(product_id, '')) = ''
              OR NULLIF(TRIM(category), '') IS NULL
              OR mrp_number IS NULL OR mrp_number <= 0
              OR cost_number IS NULL OR cost_number <= 0
              OR launch_day IS NULL OR key_count > 1 THEN 'FAIL'
            WHEN cost_number > mrp_number OR launch_day > CURRENT_DATE THEN 'WARN'
            ELSE 'PASS' END AS dq_status,
       CONCAT_WS('; ',
         CASE WHEN TRIM(COALESCE(product_id, '')) = '' THEN 'missing product_id' END,
         CASE WHEN NULLIF(TRIM(category), '') IS NULL THEN 'missing category' END,
         CASE WHEN mrp_number IS NULL OR mrp_number <= 0 THEN 'invalid mrp' END,
         CASE WHEN cost_number IS NULL OR cost_number <= 0 THEN 'invalid cost_price' END,
         CASE WHEN launch_day IS NULL THEN 'invalid launch_date' END,
         CASE WHEN key_count > 1 THEN 'duplicate product_id' END
       ) AS failure_reason,
       CONCAT_WS('; ',
         CASE WHEN cost_number > mrp_number THEN 'cost_price exceeds mrp' END,
         CASE WHEN launch_day > CURRENT_DATE THEN 'future launch_date' END
       ) AS warning_reason,
       _loaded_at, _source_file, _run_id
FROM latest_products;

CREATE TABLE IF NOT EXISTS stg_stores (
    store_id VARCHAR,
    store_name VARCHAR,
    zone VARCHAR,
    area VARCHAR,
    pincode VARCHAR,
    open_date DATE,
    capacity_orders_per_day BIGINT,
    dq_status VARCHAR,
    failure_reason VARCHAR,
    warning_reason VARCHAR,
    _loaded_at TIMESTAMPTZ,
    _source_file VARCHAR,
    _run_id VARCHAR
);

CREATE OR REPLACE TEMP TABLE latest_stores AS
WITH current_source AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY store_id, _run_id) AS key_count,
           ROW_NUMBER() OVER (
               PARTITION BY store_id ORDER BY _loaded_at DESC, _run_id DESC
           ) AS latest_row
    FROM raw_stores
)
SELECT *, TRY_CAST(open_date AS DATE) AS open_day,
       TRY_CAST(capacity_orders_per_day AS BIGINT) AS capacity_number
FROM current_source
WHERE latest_row = 1;

CREATE OR REPLACE TABLE stg_stores AS
SELECT store_id, store_name, UPPER(TRIM(zone)) AS zone, area, pincode,
       open_day AS open_date, capacity_number AS capacity_orders_per_day,
       CASE WHEN TRIM(COALESCE(store_id, '')) = ''
              OR TRIM(COALESCE(store_name, '')) = ''
              OR NULLIF(TRIM(zone), '') IS NULL
              OR capacity_number IS NULL OR capacity_number <= 0
              OR open_day IS NULL OR key_count > 1 THEN 'FAIL'
            WHEN NOT REGEXP_FULL_MATCH(TRIM(pincode), '[0-9]{6}')
              OR open_day > CURRENT_DATE THEN 'WARN'
            ELSE 'PASS' END AS dq_status,
       CONCAT_WS('; ',
         CASE WHEN TRIM(COALESCE(store_id, '')) = '' THEN 'missing store_id' END,
         CASE WHEN TRIM(COALESCE(store_name, '')) = '' THEN 'missing store_name' END,
         CASE WHEN NULLIF(TRIM(zone), '') IS NULL THEN 'missing zone' END,
         CASE WHEN capacity_number IS NULL OR capacity_number <= 0 THEN 'invalid capacity' END,
         CASE WHEN open_day IS NULL THEN 'invalid open_date' END,
         CASE WHEN key_count > 1 THEN 'duplicate store_id' END
       ) AS failure_reason,
       CONCAT_WS('; ',
         CASE WHEN NOT REGEXP_FULL_MATCH(TRIM(pincode), '[0-9]{6}') THEN 'invalid pincode' END,
         CASE WHEN open_day > CURRENT_DATE THEN 'future open_date' END
       ) AS warning_reason,
       _loaded_at, _source_file, _run_id
FROM latest_stores;

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id VARCHAR,
    order_ts TIMESTAMPTZ,
    order_date DATE,
    customer_id VARCHAR,
    store_id VARCHAR,
    product_id VARCHAR,
    quantity BIGINT,
    unit_price DOUBLE,
    discount_pct DOUBLE,
    order_amount DOUBLE,
    payment_mode VARCHAR,
    order_status VARCHAR,
    channel VARCHAR,
    delivery_minutes DOUBLE,
    dq_status VARCHAR,
    failure_reason VARCHAR,
    warning_reason VARCHAR,
    _source_file VARCHAR,
    source_row BIGINT,
    _loaded_at TIMESTAMPTZ,
    _run_id VARCHAR
);

CREATE OR REPLACE TEMP TABLE current_orders AS
WITH source_rows AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY order_id, _source_file, _run_id) AS key_count,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY CASE WHEN _source_file LIKE '%increment%' THEN 1 ELSE 0 END DESC,
                        _loaded_at DESC
           ) AS latest_row
    FROM raw_orders
),
typed AS (
    SELECT order_id,
           TRY_CAST(order_ts AS TIMESTAMP) AT TIME ZONE 'Asia/Kolkata' AS order_ts,
           TRY_CAST(order_ts AS DATE) AS order_date,
           customer_id, store_id, product_id,
           TRY_CAST(quantity AS BIGINT) AS quantity,
           TRY_CAST(unit_price AS DOUBLE) AS unit_price,
           TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
           TRY_CAST(order_amount AS DOUBLE) AS order_amount,
           CASE
             WHEN UPPER(TRIM(payment_mode)) = 'COD' THEN 'CASH ON DELIVERY'
             ELSE UPPER(TRIM(payment_mode))
           END AS payment_mode,
           UPPER(TRIM(order_status)) AS order_status,
           channel, TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
           key_count, _source_file, _source_row AS source_row,
           _loaded_at, _run_id
    FROM source_rows
    WHERE latest_row = 1
)
SELECT *,
       CONCAT_WS('; ',
         CASE WHEN NULLIF(TRIM(order_id), '') IS NULL THEN 'missing order_id' END,
         CASE WHEN NULLIF(TRIM(customer_id), '') IS NULL THEN 'missing customer_id' END,
         CASE WHEN NULLIF(TRIM(store_id), '') IS NULL THEN 'missing store_id' END,
         CASE WHEN NULLIF(TRIM(product_id), '') IS NULL THEN 'missing product_id' END,
         CASE WHEN order_ts IS NULL THEN 'invalid order_ts' END,
         CASE WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED')
              THEN 'invalid order_status' END,
         CASE WHEN payment_mode NOT IN ('UPI', 'CARD', 'CASH ON DELIVERY', 'WALLET')
              THEN 'invalid payment_mode' END,
         CASE WHEN quantity IS NULL OR quantity <= 0 THEN 'invalid quantity' END,
         CASE WHEN discount_pct IS NULL OR discount_pct < 0 OR discount_pct > 100
              THEN 'invalid discount_pct' END,
         CASE WHEN order_amount IS NULL OR unit_price IS NULL OR quantity IS NULL
                   OR ABS(order_amount - ROUND(quantity * unit_price * (1-discount_pct/100), 2)) > 0.01
              THEN 'order_amount formula mismatch' END,
         CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL
              THEN 'missing delivery_minutes' END,
         CASE WHEN order_status = 'DELIVERED' AND delivery_minutes >= 120
              THEN 'delivery_minutes is at least 120' END,
         CASE WHEN key_count > 1 THEN 'duplicate order_id in source file' END,
         CASE WHEN order_ts > CURRENT_TIMESTAMP THEN 'future order_ts' END,
         CASE WHEN customer_id NOT IN (SELECT customer_id FROM stg_customers WHERE dq_status <> 'FAIL')
              THEN 'unknown or rejected customer_id' END,
         CASE WHEN product_id NOT IN (SELECT product_id FROM stg_products WHERE dq_status <> 'FAIL')
              THEN 'unknown or rejected product_id' END,
         CASE WHEN store_id NOT IN (SELECT store_id FROM stg_stores WHERE dq_status <> 'FAIL')
              THEN 'unknown or rejected store_id' END
       ) AS failure_reason,
       '' AS warning_reason
FROM typed;

MERGE INTO stg_orders AS target
USING (
    SELECT order_id, order_ts, order_date, customer_id, store_id, product_id,
           quantity, unit_price, discount_pct, order_amount, payment_mode,
           order_status, channel, delivery_minutes,
           CASE WHEN failure_reason <> '' THEN 'FAIL' ELSE 'PASS' END AS dq_status,
           failure_reason, warning_reason, _source_file, source_row, _loaded_at, _run_id
    FROM current_orders
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET
    order_ts=source.order_ts, order_date=source.order_date,
    customer_id=source.customer_id, store_id=source.store_id,
    product_id=source.product_id, quantity=source.quantity,
    unit_price=source.unit_price, discount_pct=source.discount_pct,
    order_amount=source.order_amount, payment_mode=source.payment_mode,
    order_status=source.order_status, channel=source.channel,
    delivery_minutes=source.delivery_minutes, dq_status=source.dq_status,
    failure_reason=source.failure_reason, warning_reason=source.warning_reason,
    _source_file=source._source_file, source_row=source.source_row,
    _loaded_at=source._loaded_at, _run_id=source._run_id
WHEN NOT MATCHED THEN INSERT (
    order_id, order_ts, order_date, customer_id, store_id, product_id,
    quantity, unit_price, discount_pct, order_amount, payment_mode,
    order_status, channel, delivery_minutes, dq_status, failure_reason,
    warning_reason, _source_file, source_row, _loaded_at, _run_id
) VALUES (
    source.order_id, source.order_ts, source.order_date, source.customer_id,
    source.store_id, source.product_id, source.quantity, source.unit_price,
    source.discount_pct, source.order_amount, source.payment_mode,
    source.order_status, source.channel, source.delivery_minutes,
    source.dq_status, source.failure_reason, source.warning_reason,
    source._source_file, source.source_row, source._loaded_at, source._run_id
);

CREATE TABLE IF NOT EXISTS stg_payments (
    payment_id VARCHAR,
    order_id VARCHAR,
    payment_ts TIMESTAMPTZ,
    payment_mode VARCHAR,
    amount_paid DOUBLE,
    payment_status VARCHAR,
    gateway_ref VARCHAR,
    dq_status VARCHAR,
    failure_reason VARCHAR,
    warning_reason VARCHAR,
    _source_file VARCHAR,
    _loaded_at TIMESTAMPTZ,
    _run_id VARCHAR
);

CREATE OR REPLACE TABLE stg_payments AS
WITH ranked AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY payment_id, _run_id) AS key_count,
           ROW_NUMBER() OVER (
               PARTITION BY payment_id ORDER BY _loaded_at DESC
           ) AS latest_row
    FROM raw_payments
),
typed AS (
    SELECT payment_id, order_id,
           TRY_CAST(payment_ts AS TIMESTAMP) AT TIME ZONE 'Asia/Kolkata' AS payment_ts,
           CASE WHEN UPPER(TRIM(payment_mode)) = 'COD' THEN 'CASH ON DELIVERY'
                ELSE UPPER(TRIM(payment_mode)) END AS payment_mode,
           TRY_CAST(amount_paid AS DOUBLE) AS amount_paid,
           UPPER(TRIM(payment_status)) AS payment_status, gateway_ref,
           key_count, _source_file, _loaded_at, _run_id
    FROM ranked
    WHERE latest_row = 1
)
SELECT payment_id, order_id, payment_ts, payment_mode, amount_paid,
       payment_status, gateway_ref,
       CASE WHEN NULLIF(TRIM(payment_id), '') IS NULL
              OR NULLIF(TRIM(order_id), '') IS NULL
              OR payment_ts IS NULL OR amount_paid IS NULL OR amount_paid < 0
              OR payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED')
              OR payment_mode NOT IN ('UPI', 'CARD', 'CASH ON DELIVERY', 'WALLET')
              OR key_count > 1 OR payment_ts > CURRENT_TIMESTAMP
              OR order_id NOT IN (SELECT order_id FROM stg_orders)
              OR (payment_status = 'SUCCESS' AND EXISTS (
                   SELECT 1 FROM stg_orders o
                   WHERE o.order_id = typed.order_id
                     AND ABS(o.order_amount - typed.amount_paid) > 0.01
              ))
            THEN 'FAIL' ELSE 'PASS' END AS dq_status,
       CONCAT_WS('; ',
         CASE WHEN NULLIF(TRIM(payment_id), '') IS NULL THEN 'missing payment_id' END,
         CASE WHEN NULLIF(TRIM(order_id), '') IS NULL THEN 'missing order_id' END,
         CASE WHEN payment_ts IS NULL THEN 'invalid payment_ts' END,
         CASE WHEN amount_paid IS NULL OR amount_paid < 0 THEN 'invalid amount_paid' END,
         CASE WHEN payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') THEN 'invalid payment_status' END,
         CASE WHEN payment_mode NOT IN ('UPI', 'CARD', 'CASH ON DELIVERY', 'WALLET') THEN 'invalid payment_mode' END,
         CASE WHEN key_count > 1 THEN 'duplicate payment_id' END,
         CASE WHEN payment_ts > CURRENT_TIMESTAMP THEN 'future payment_ts' END,
         CASE WHEN order_id NOT IN (SELECT order_id FROM stg_orders) THEN 'unknown order_id' END,
         CASE WHEN payment_status = 'SUCCESS' AND EXISTS (
                   SELECT 1 FROM stg_orders o WHERE o.order_id = typed.order_id
                     AND ABS(o.order_amount - typed.amount_paid) > 0.01
              ) THEN 'SUCCESS amount mismatch' END
       ) AS failure_reason,
       CASE WHEN NULLIF(TRIM(gateway_ref), '') IS NULL THEN 'missing gateway_ref' ELSE '' END
           AS warning_reason,
       _source_file, _loaded_at, _run_id
FROM typed;

INSERT INTO dq_results
SELECT c.run_id, c.dag_id, checks.check_name, checks.dimension,
       checks.severity, checks.rows_checked, checks.rows_failed,
       CASE WHEN checks.rows_failed = 0 THEN 'PASS'
            WHEN checks.severity = 'WARNING' THEN 'WARN' ELSE 'FAIL' END,
       c.checked_at
FROM pipeline_context c
CROSS JOIN (
    SELECT 'orders_completeness_required_fields' AS check_name, 'completeness' AS dimension,
           'CRITICAL' AS severity, COUNT(*) AS rows_checked,
           COUNT(*) FILTER (WHERE NULLIF(TRIM(customer_id), '') IS NULL
                              OR NULLIF(TRIM(store_id), '') IS NULL
                              OR NULLIF(TRIM(product_id), '') IS NULL
                              OR (UPPER(TRIM(order_status))='DELIVERED'
                                  AND NULLIF(TRIM(delivery_minutes), '') IS NULL)) AS rows_failed
    FROM raw_orders WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'orders_uniqueness_order_id', 'uniqueness', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE dups > 1)
    FROM (SELECT order_id, _source_file, COUNT(*) AS dups FROM raw_orders
          WHERE _run_id=(SELECT run_id FROM pipeline_context)
          GROUP BY order_id, _source_file) d
    UNION ALL
    SELECT 'orders_validity_status_mode_timestamp', 'validity', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE UPPER(TRIM(order_status)) NOT IN ('DELIVERED','CANCELLED','RETURNED')
                              OR UPPER(TRIM(payment_mode)) NOT IN ('UPI','CARD','CASH ON DELIVERY','WALLET')
                              OR (TRY_CAST(order_ts AS TIMESTAMP) AT TIME ZONE 'Asia/Kolkata') IS NULL)
    FROM raw_orders WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'orders_accuracy_amount_quantity_discount', 'accuracy', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE TRY_CAST(quantity AS BIGINT) IS NULL
                              OR TRY_CAST(quantity AS BIGINT) <= 0
                              OR TRY_CAST(discount_pct AS DOUBLE) IS NULL
                              OR TRY_CAST(discount_pct AS DOUBLE) NOT BETWEEN 0 AND 100
                              OR TRY_CAST(order_amount AS DOUBLE) IS NULL
                              OR TRY_CAST(unit_price AS DOUBLE) IS NULL
                              OR ABS(TRY_CAST(order_amount AS DOUBLE)
                                  - ROUND(TRY_CAST(quantity AS DOUBLE)*TRY_CAST(unit_price AS DOUBLE)
                                      *(1-TRY_CAST(discount_pct AS DOUBLE)/100),2)) > 0.01)
    FROM raw_orders WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'orders_consistency_master_keys', 'consistency', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE customer_id NOT IN (SELECT customer_id FROM stg_customers WHERE dq_status <> 'FAIL')
                              OR product_id NOT IN (SELECT product_id FROM stg_products WHERE dq_status <> 'FAIL')
                              OR store_id NOT IN (SELECT store_id FROM stg_stores WHERE dq_status <> 'FAIL'))
    FROM raw_orders WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'orders_timeliness_future_timestamp', 'timeliness', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE
               (TRY_CAST(order_ts AS TIMESTAMP) AT TIME ZONE 'Asia/Kolkata') > CURRENT_TIMESTAMP)
    FROM raw_orders WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'orders_timeliness_stale_snapshot', 'timeliness', 'WARNING', COUNT(*),
           CASE WHEN MAX(TRY_CAST(order_ts AS TIMESTAMP) AT TIME ZONE 'Asia/Kolkata')
                     < CURRENT_TIMESTAMP - INTERVAL '1 day'
                THEN COUNT(*) ELSE 0 END
    FROM raw_orders WHERE _run_id=(SELECT run_id FROM pipeline_context)
      AND _source_file='orders.csv'
    UNION ALL
    SELECT 'orders_volume_anomaly', 'volume', 'WARNING', COUNT(*),
           CASE WHEN COUNT(*) < 0.1 * COALESCE((
               SELECT prior.rows_checked FROM dq_results prior
               WHERE prior.dag_id=(SELECT dag_id FROM pipeline_context)
                 AND prior.check_name='orders_volume_anomaly'
                 AND prior.status='PASS'
                 AND prior.run_id <> (SELECT run_id FROM pipeline_context)
               ORDER BY prior.checked_at DESC LIMIT 1
           ), 0) THEN COUNT(*) ELSE 0 END
    FROM raw_orders WHERE _run_id=(SELECT run_id FROM pipeline_context)
      AND _source_file='orders.csv'
    UNION ALL
    SELECT 'orders_distribution_unit_price', 'distribution', 'WARNING', COUNT(*),
           COUNT(*) FILTER (WHERE TRY_CAST(unit_price AS DOUBLE) > 5 * (
               SELECT MEDIAN(TRY_CAST(prior.unit_price AS DOUBLE))
               FROM raw_orders prior
               WHERE prior._run_id = (
                   SELECT _run_id FROM raw_orders
                   WHERE _run_id <> (SELECT run_id FROM pipeline_context)
                     AND _source_file='orders.csv'
                   ORDER BY _loaded_at DESC LIMIT 1
               )
                 AND prior.product_id = current.product_id
                 AND TRY_CAST(prior.unit_price AS DOUBLE) > 0
           ))
    FROM raw_orders current WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'customers_completeness_required_fields', 'completeness', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE NULLIF(TRIM(customer_id),'') IS NULL
                              OR NULLIF(TRIM(full_name),'') IS NULL)
    FROM raw_customers WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'customers_uniqueness_customer_id', 'uniqueness', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE dups > 1)
    FROM (SELECT customer_id, COUNT(*) AS dups FROM raw_customers
          WHERE _run_id=(SELECT run_id FROM pipeline_context)
          GROUP BY customer_id) d
    UNION ALL
    SELECT 'customers_validity_email_format', 'validity', 'WARNING', COUNT(*),
           COUNT(*) FILTER (WHERE NOT REGEXP_FULL_MATCH(TRIM(email), '[^@[:space:]]+@[^@[:space:]]+[.][^@[:space:]]+'))
    FROM raw_customers WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'products_accuracy_price_bounds', 'accuracy', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE TRY_CAST(mrp AS DOUBLE) IS NULL OR TRY_CAST(mrp AS DOUBLE) <= 0
                              OR TRY_CAST(cost_price AS DOUBLE) IS NULL
                              OR TRY_CAST(cost_price AS DOUBLE) <= 0)
    FROM raw_products WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'stores_completeness_zone', 'completeness', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE NULLIF(TRIM(zone),'') IS NULL)
    FROM raw_stores WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'payments_consistency_success_amount', 'consistency', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE UPPER(TRIM(payment_status))='SUCCESS'
                              AND EXISTS (SELECT 1 FROM stg_orders o
                                          WHERE o.order_id=raw_payments.order_id
                                            AND ABS(o.order_amount-TRY_CAST(raw_payments.amount_paid AS DOUBLE)) > 0.01))
    FROM raw_payments WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'orders_accuracy_delivery_time', 'accuracy', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE UPPER(TRIM(order_status))='DELIVERED'
                              AND (NULLIF(TRIM(delivery_minutes),'') IS NULL
                                   OR TRY_CAST(delivery_minutes AS DOUBLE) >= 120))
    FROM raw_orders WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'customers_validity_phone_format', 'validity', 'WARNING', COUNT(*),
           COUNT(*) FILTER (WHERE NOT REGEXP_FULL_MATCH(TRIM(phone), '[0-9]{10}'))
    FROM raw_customers WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'customers_validity_pincode_format', 'validity', 'WARNING', COUNT(*),
           COUNT(*) FILTER (WHERE NOT REGEXP_FULL_MATCH(TRIM(pincode), '[0-9]{6}'))
    FROM raw_customers WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'customers_timeliness_signup_date', 'timeliness', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE TRY_CAST(signup_date AS DATE) IS NULL
                              OR TRY_CAST(signup_date AS DATE) > CURRENT_DATE)
    FROM raw_customers WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'products_completeness_category', 'completeness', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE NULLIF(TRIM(product_id),'') IS NULL
                              OR NULLIF(TRIM(category),'') IS NULL)
    FROM raw_products WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'products_uniqueness_product_id', 'uniqueness', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE dups > 1)
    FROM (SELECT product_id, COUNT(*) AS dups FROM raw_products
          WHERE _run_id=(SELECT run_id FROM pipeline_context)
          GROUP BY product_id) d
    UNION ALL
    SELECT 'products_accuracy_cost_price_mrp', 'accuracy', 'WARNING', COUNT(*),
           COUNT(*) FILTER (WHERE TRY_CAST(cost_price AS DOUBLE) > TRY_CAST(mrp AS DOUBLE))
    FROM raw_products WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'products_validity_launch_date', 'validity', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE TRY_CAST(launch_date AS DATE) IS NULL)
    FROM raw_products WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'products_timeliness_launch_date', 'timeliness', 'WARNING', COUNT(*),
           COUNT(*) FILTER (WHERE TRY_CAST(launch_date AS DATE) > CURRENT_DATE)
    FROM raw_products WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'stores_uniqueness_store_id', 'uniqueness', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE dups > 1)
    FROM (SELECT store_id, COUNT(*) AS dups FROM raw_stores
          WHERE _run_id=(SELECT run_id FROM pipeline_context)
          GROUP BY store_id) d
    UNION ALL
    SELECT 'stores_accuracy_capacity', 'accuracy', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE TRY_CAST(capacity_orders_per_day AS BIGINT) IS NULL
                              OR TRY_CAST(capacity_orders_per_day AS BIGINT) <= 0)
    FROM raw_stores WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'stores_validity_pincode', 'validity', 'WARNING', COUNT(*),
           COUNT(*) FILTER (WHERE NOT REGEXP_FULL_MATCH(TRIM(pincode), '[0-9]{6}'))
    FROM raw_stores WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'stores_timeliness_open_date', 'timeliness', 'WARNING', COUNT(*),
           COUNT(*) FILTER (WHERE TRY_CAST(open_date AS DATE) > CURRENT_DATE)
    FROM raw_stores WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'payments_completeness_required_fields', 'completeness', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE NULLIF(TRIM(payment_id),'') IS NULL
                              OR NULLIF(TRIM(order_id),'') IS NULL
                              OR NULLIF(TRIM(amount_paid),'') IS NULL)
    FROM raw_payments WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'payments_uniqueness_payment_id', 'uniqueness', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE dups > 1)
    FROM (SELECT payment_id, COUNT(*) AS dups FROM raw_payments
          WHERE _run_id=(SELECT run_id FROM pipeline_context)
          GROUP BY payment_id) d
    UNION ALL
    SELECT 'payments_validity_status_mode_timestamp', 'validity', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE UPPER(TRIM(payment_status)) NOT IN ('SUCCESS','FAILED','REFUNDED')
                              OR UPPER(TRIM(payment_mode)) NOT IN ('UPI','CARD','CASH ON DELIVERY','WALLET')
                              OR (TRY_CAST(payment_ts AS TIMESTAMP) AT TIME ZONE 'Asia/Kolkata') IS NULL)
    FROM raw_payments WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'payments_accuracy_amount', 'accuracy', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE TRY_CAST(amount_paid AS DOUBLE) IS NULL
                              OR TRY_CAST(amount_paid AS DOUBLE) < 0)
    FROM raw_payments WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'payments_consistency_order_key', 'consistency', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE order_id NOT IN (SELECT order_id FROM stg_orders))
    FROM raw_payments WHERE _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT 'payments_timeliness_payment_timestamp', 'timeliness', 'CRITICAL', COUNT(*),
           COUNT(*) FILTER (WHERE
               (TRY_CAST(payment_ts AS TIMESTAMP) AT TIME ZONE 'Asia/Kolkata') > CURRENT_TIMESTAMP)
    FROM raw_payments WHERE _run_id=(SELECT run_id FROM pipeline_context)
) checks
ON CONFLICT (run_id, dag_id, check_name) DO UPDATE SET
    dimension=excluded.dimension, severity=excluded.severity,
    rows_checked=excluded.rows_checked, rows_failed=excluded.rows_failed,
    status=excluded.status, checked_at=excluded.checked_at;
