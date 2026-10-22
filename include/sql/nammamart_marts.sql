CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT o.order_date, s.zone, o.store_id, s.store_name,
       COUNT(*) AS delivered_orders,
       ROUND(SUM(o.order_amount), 2) AS net_revenue
FROM stg_orders o
JOIN stg_stores s USING (store_id)
WHERE o.dq_status <> 'FAIL'
  AND s.dq_status <> 'FAIL'
  AND o.order_status = 'DELIVERED'
GROUP BY o.order_date, s.zone, o.store_id, s.store_name;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category, COUNT(*) AS delivered_orders,
       SUM(o.quantity) AS units_sold,
       ROUND(SUM(o.order_amount), 2) AS net_revenue,
       ROUND(SUM(o.order_amount - o.quantity * p.cost_price), 2) AS gross_margin
FROM stg_orders o
JOIN stg_products p USING (product_id)
WHERE o.dq_status <> 'FAIL'
  AND p.dq_status <> 'FAIL'
  AND o.order_status = 'DELIVERED'
GROUP BY p.category;

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT o.store_id, s.store_name,
       ROUND(AVG(o.delivery_minutes), 2) AS avg_delivery_minutes,
       COUNT(*) FILTER (WHERE o.delivery_minutes <= 15) AS within_15_orders,
       COUNT(*) AS delivered_orders,
       ROUND(100.0 * COUNT(*) FILTER (WHERE o.delivery_minutes <= 15)
             / NULLIF(COUNT(*), 0), 2) AS within_15_pct
FROM stg_orders o
JOIN stg_stores s USING (store_id)
WHERE o.dq_status <> 'FAIL'
  AND s.dq_status <> 'FAIL'
  AND o.order_status = 'DELIVERED'
GROUP BY o.store_id, s.store_name;

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH successful_payments AS (
    SELECT order_id, COUNT(*) AS payment_count,
           SUM(amount_paid) AS amount_paid,
           STRING_AGG(DISTINCT payment_mode, ', ') AS payment_mode
    FROM stg_payments
    WHERE dq_status <> 'FAIL' AND payment_status = 'SUCCESS'
    GROUP BY order_id
),
delivered_orders AS (
    SELECT o.order_id, o.order_date, o.payment_mode AS order_payment_mode,
           o.order_amount, p.payment_count, p.amount_paid,
           p.payment_mode AS gateway_payment_mode
    FROM stg_orders o
    LEFT JOIN successful_payments p USING (order_id)
    WHERE o.dq_status <> 'FAIL' AND o.order_status = 'DELIVERED'
)
SELECT order_id, order_date,
       order_payment_mode, gateway_payment_mode,
       order_amount, amount_paid,
       CASE
         WHEN payment_count IS NULL THEN 'missing_successful_payment'
         WHEN payment_count > 1 THEN 'multiple_successful_payments'
         WHEN ABS(COALESCE(amount_paid, 0) - order_amount) > 0.01 THEN 'amount_mismatch'
         WHEN UPPER(order_payment_mode) <> UPPER(gateway_payment_mode) THEN 'payment_mode_mismatch'
       END AS mismatch_reason
FROM delivered_orders
WHERE payment_count IS NULL OR payment_count <> 1
   OR ABS(COALESCE(amount_paid, 0) - order_amount) > 0.01
   OR UPPER(order_payment_mode) <> UPPER(gateway_payment_mode)
UNION ALL
SELECT p.order_id, NULL, '', p.payment_mode, NULL, p.amount_paid,
       'payment_without_delivered_order'
FROM stg_payments p
LEFT JOIN stg_orders o USING (order_id)
WHERE p.dq_status <> 'FAIL' AND p.payment_status = 'SUCCESS'
  AND (o.order_id IS NULL OR o.dq_status = 'FAIL' OR o.order_status <> 'DELIVERED');

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT o.customer_id, c.loyalty_tier, c.masked_email,
       ROUND(SUM(o.order_amount), 2) AS total_spend
FROM stg_orders o
JOIN stg_customers c USING (customer_id)
WHERE o.dq_status <> 'FAIL'
  AND c.dq_status <> 'FAIL'
  AND o.order_status = 'DELIVERED'
GROUP BY o.customer_id, c.loyalty_tier, c.masked_email
ORDER BY total_spend DESC
LIMIT 10;

CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
SELECT payment_mode, COUNT(*) AS delivered_orders,
       ROUND(SUM(order_amount), 2) AS net_revenue
FROM stg_orders
WHERE dq_status <> 'FAIL' AND order_status = 'DELIVERED'
GROUP BY payment_mode;

CREATE OR REPLACE TABLE mart_rejected_rows_by_file_reason AS
SELECT _source_file AS source_file, failure_reason,
       COUNT(*) AS rejected_rows
FROM (
    SELECT _source_file, failure_reason FROM stg_orders
    WHERE dq_status = 'FAIL' AND _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT _source_file, failure_reason FROM stg_payments
    WHERE dq_status = 'FAIL' AND _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT _source_file, failure_reason FROM stg_customers
    WHERE dq_status = 'FAIL' AND _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT _source_file, failure_reason FROM stg_products
    WHERE dq_status = 'FAIL' AND _run_id=(SELECT run_id FROM pipeline_context)
    UNION ALL
    SELECT _source_file, failure_reason FROM stg_stores
    WHERE dq_status = 'FAIL' AND _run_id=(SELECT run_id FROM pipeline_context)
) rejected
GROUP BY _source_file, failure_reason;
