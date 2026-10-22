# NammaMart ETL/ELT Submission

## Implementation approach and status

1. **Profile first:** all five September exports and the October increment were profiled before running controlled Airflow experiments. The experiment inputs were restored byte-for-byte afterward. The machine-readable profile and row-level issue log are in [`include/profile/nammamart_profile.json`](include/profile/nammamart_profile.json) and [`include/profile/nammamart_data_issues.csv`](include/profile/nammamart_data_issues.csv).
2. **Implement common quality rules:** Python validation is shared by ETL, while ELT applies corresponding casts and checks in DuckDB SQL. Critical rows are excluded from ETL reporting facts; ELT retains every source row in raw and marks rejected staging rows.
3. **Load and report:** ETL writes an idempotent star schema; ELT appends text-only raw data, MERGEs current orders by `order_id`, and builds marts using SQL files.
4. **Audit and compare:** both paths write `dq_results` and `pipeline_runs`, include source-file/row lineage, and calculate the same September revenue and payment-reconciliation results in local tests.
5. **Run and observe in Airflow:** Docker Desktop, WSL 2 and Astro CLI were installed and used to run the local Airflow UI. Both DAGs imported without errors, completed successful runs, and were exercised against controlled failure and data-quality cases. Results for all ten experiments are below. An Airflow 3 runtime incompatibility in the audit tasks was fixed during the live run and then verified with failure and recovery runs.

The DAGs use the generic project suffix `pipeline` because no submitter name was provided. Both target `include/warehouse_nammamart_pipeline.duckdb`. A file lock serializes database writers because the two DAGs share one embedded DuckDB file.

## Part 0 — profiling and data issues

The six files contain **5,372 rows** in total. Counts below are from the supplied files. A duplicate-key row count includes every row participating in a repeated key; the increment's 10 keys that also occur in `orders.csv` are expected updates, not within-file duplicates.

| File | Rows | Blank columns and counts | Repeated-key rows / keys |
|---|---:|---|---|
| `orders.csv` | 2,412 | `order_ts` 1, `customer_id` 18, `delivery_minutes` 164 | 24 / 12 |
| `payments.csv` | 2,228 | `gateway_ref` 301 | 8 / 4 |
| `customers.csv` | 405 | `city` 6 | 10 / 5 |
| `products.csv` | 152 | `category` 3 | 4 / 2 |
| `stores.csv` | 15 | `zone` 1 | 0 / 0 |
| `orders_2026-10-01_increment.csv` | 160 | none | 0 / 0 |

Numeric and date ranges, exact repeated keys, category values and per-column blanks are recorded in `nammamart_profile.json`. Notable ranges: orders `quantity` -3–5, `unit_price` 10–883, `discount_pct` 0–250, `order_amount` -1,593–9,576 and `delivery_minutes` 8–1,440; payments `amount_paid` -1,593–9,576; product `mrp` -203–883 and `cost_price` 6.79–631.79; store capacity 0–600. Order timestamps parse to a range of 2026-09-01 through 2027-01-01; payment timestamps range from 2026-09-01 through 2027-01-01; customer signup dates range from 2024-06-01 through 2027-01-05.

Distinct business categories include order statuses `CANCELLED`, `DELIVERED`, `Pending??`, `RETURNED`, and lowercase `delivered`; payment modes include `CARD`, `Card`, `Cash on Delivery`, `Cashh`, `UPI`, `Wallet`, `cod`, and `upi`; loyalty tiers have mixed casing (`BRONZE`/`Bronze`/`bronze`, `SILVER`/`Silver`, `Gold`, `Platinum`); zones are East, North, South and West; products contain six named categories plus three blank values.

### Data Issues Log (summary)

| File | Observed issue | Dimension / severity | Rule applied |
|---|---|---|---|
| Orders | 18 missing customer IDs; 24 rows in 12 duplicate order keys; six invalid/missing timestamps and five future timestamps; three invalid statuses, two invalid payment modes; 10 non-positive quantities, three out-of-range discounts, 18 amount-formula mismatches; eight delivered rows missing delivery time and six at/over 120 minutes | Completeness, uniqueness, validity, accuracy, timeliness / Critical | Required identifiers and timestamps, unique order ID, allowed lists, positive quantity, 0–100 discount, amount formula within 0.01, delivered time required and below 120 minutes, no future order |
| Orders | 65 customer, 103 product and 316 store consistency failures in the baseline rows | Consistency / Critical | Foreign keys must refer to a unique, accepted master record. This includes cascades from rejected master keys, not just IDs absent from the raw master export. |
| Payments | Four duplicate payment IDs (eight rows), six negative amounts, eight unknown order IDs, 14 successful amount mismatches, two invalid modes and five future payment timestamps | Uniqueness, accuracy, consistency, validity, timeliness / Critical | Unique payment ID; non-negative amount; known order; SUCCESS amount equals its order; allowed payment mode/status; no future payment time |
| Customers | Five duplicate IDs (10 rows), three future signup dates, 12 malformed/blank emails, seven invalid phones, three invalid pincodes and mixed loyalty-tier casing | Uniqueness and future dates / Critical; format and casing / Warning | Keep PII format/casing warnings loadable but masked; reject missing/ambiguous keys and future signup records |
| Products | Two duplicate product IDs (four rows), three missing categories, one invalid/non-positive MRP and five cost-above-MRP rows | Uniqueness, completeness and invalid MRP / Critical; cost/MRP / Warning | Unique key, category required, positive prices; review rather than reject cost-above-MRP |
| Stores | One missing zone and one non-positive capacity | Completeness and accuracy / Critical | Store key/name/zone required; capacity must be positive |
| Increment | All 160 rows have unique IDs; 10 IDs overlap the base file as updates. 23 rows reference rejected/ambiguous master keys and are quarantined by ETL. | Consistency / Critical | Stage all increment records and preserve raw history; only valid current-state records enter ETL facts. |

The CSV issue log lists each detected row key, column, problem, quality dimension, proposed rule and severity; one row can have multiple issues. Three questions to confirm with the client:

1. Should a duplicate master key always make every referencing order unusable, or is there an authoritative “latest record wins” rule?
2. Are free/zero-price orders and cost-above-MRP promotions legitimate, and should those be exceptions or warnings?
3. What is the authoritative timezone of source timestamps, and what delivery/freshness limits should trigger a business warning versus rejection?

## Parts A–D — pipeline design, schedules and quality

### ETL

`nammamart_etl_pipeline` extracts the five base files in parallel plus the optional increment. Python validates each source, writes critical failures to `include/quarantine/nammamart/<file>_failed_<run_id>.csv` with `failure_reason`, records row-level findings in `dq_issue_details`, masks customer email/phone, removes names from reporting dimensions, joins accepted dimensions, and derives net revenue, gross margin and delivery SLA fields. Replacing the current fact/dimension snapshots by valid order keys makes repeat loads idempotent. The current fact keeps `_source_file`, `source_row` and `_run_id` for lineage.

### ELT

`nammamart_elt_pipeline` loads the five base CSVs and optional increment through Python's CSV reader without pandas cleaning. `raw_orders`, `raw_payments`, `raw_customers`, `raw_products` and `raw_stores` retain source fields as text plus load/source/run metadata. The SQL files [`nammamart_staging.sql`](include/sql/nammamart_staging.sql) and [`nammamart_marts.sql`](include/sql/nammamart_marts.sql) cast and validate staging data and build marts. The order MERGE prefers increment rows over the September snapshot, giving exactly **150 new IDs and 10 corrected IDs** in staging; it produces 2,550 unique staged order keys. The raw order table has 2,572 rows after one run because it contains all 2,412 base rows and all 160 increment rows.

### Scheduling and DQ

- Cron: `0 9 * * 1-6` (09:00 Monday through Saturday).
- DAG `start_date`: timezone-aware `Asia/Kolkata`; `catchup=False` avoids replaying old scheduled intervals when the local demo is started.
- 09:00 IST is **03:30 UTC** in Airflow's UTC display.
- The configured 09:00 IST start time does **not** guarantee report availability before 09:00; task execution begins at that time. To meet the client's readiness SLA, the production start time must be moved earlier by an agreed completion buffer (or the SLA must be clarified as a 09:00 start). This assignment's explicit 09:00 schedule is retained for rubric compliance, so readiness before 09:00 remains an unresolved deployment decision rather than a demonstrated guarantee.
- Airflow's current next-run values and the latest run states are captured in the refreshed [ETL schedule screenshot](submission_assets/airflow/etl_schedule.png) and [ELT schedule screenshot](submission_assets/airflow/elt_schedule.png).
- If the cron were interpreted from a UTC `start_date`, a 09:00 UTC run would occur at 14:30 IST, missing the morning SLA.
- If the laptop/Docker is off, Airflow cannot run. With catchup disabled, it will not replay missed daily runs automatically; bring the stack back and trigger an approved manual run.
- Both DAGs define two retries, a two-minute retry delay and a 45-minute execution timeout. `audit_run` uses `all_done` so failed upstream work still gets an audit record; it then raises `AirflowFailException` when an upstream task failed, ensuring the Airflow DAG run is failed too.
- ETL writes 68 per-run quality checks; ELT writes 34 checks. Both cover completeness, uniqueness, validity, accuracy, consistency and timeliness, plus volume and unit-price distribution warnings. Critical failures are quarantined/excluded from clean marts; warnings are logged and loaded.
- `dq_results` columns: `run_id`, `dag_id`, `check_name`, `dimension`, `severity`, `rows_checked`, `rows_failed`, `status`, `checked_at`. `pipeline_runs` stores one upserted record per DAG run.

### Local verification results

The focused `unittest` contract test completed successfully. Successful Airflow UI runs verified 2,020 accepted ETL fact orders, 1,837 delivered orders, 610 baseline ETL quarantines, 2,550 staged order IDs, increment counts of 150 new/10 updated, equal ETL/ELT September revenue (**918,754.95**), and equal payment-reconciliation row counts (646). A concurrent re-run kept ETL fact and ELT staging counts stable while appending raw ELT history. The ten controlled Airflow experiments and task states are recorded in Part F.

## Business questions — paired SQL and results

The tables below were queried after restoring the supplied files and completing successful ETL and ELT runs. The daily result is explicitly filtered to September; the category, delivery, payment-mode and customer marts represent all current accepted base-plus-increment orders. Each ETL result matches its ELT counterpart.

1. **Daily net revenue by zone and store**

   ```sql
   SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
   FROM etl_daily_zone_revenue
   WHERE order_date BETWEEN '2026-09-01' AND '2026-09-30'
   ORDER BY order_date, zone, store_id;

   SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
   FROM mart_daily_store_kpis
   WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
   ORDER BY order_date, zone, store_id;
   ```

   Both queries return **384 date/zone/store rows** with September delivered net revenue of **918,754.95**. Across all current dates, both tables contain 398 rows and total 986,768.10.

2. **Revenue and gross margin by product category**

   ```sql
   SELECT category, delivered_orders, net_revenue, gross_margin
   FROM etl_category_margin ORDER BY category;

   SELECT category, delivered_orders, net_revenue, gross_margin
   FROM mart_category_margin ORDER BY category;
   ```

   | Category | Delivered orders | Net revenue | Gross margin |
   |---|---:|---:|---:|
   | Dairy & Bakery | 339 | 153,662.25 | 40,927.30 |
   | Fruits & Vegetables | 341 | 84,421.30 | 16,727.94 |
   | Household | 286 | 166,975.00 | 42,193.60 |
   | Personal Care | 291 | 185,270.70 | 41,056.17 |
   | Snacks & Beverages | 299 | 158,120.05 | 36,939.73 |
   | Staples | 281 | 238,318.80 | 52,225.07 |
   | **Total** | **1,837** | **986,768.10** | **230,069.81** |

3. **Average delivery time and within-15-minute share by store**

   ```sql
   SELECT store_id, store_name, avg_delivery_minutes, within_15_orders,
          delivered_orders, within_15_pct
   FROM etl_delivery_sla ORDER BY store_id;

   SELECT store_id, store_name, avg_delivery_minutes, within_15_orders,
          delivered_orders, within_15_pct
   FROM mart_delivery_sla ORDER BY store_id;
   ```

   Both return the same 13 stores. The weighted average is **21.31 minutes**; **521 of 1,837 orders (28.36%)** met the 15-minute target.

   | Store | Average minutes | Within 15 min | Delivered | Within 15 % |
   |---|---:|---:|---:|---:|
   | S01 Hebbal | 20.58 | 44 | 142 | 30.99% |
   | S02 Yelahanka | 20.71 | 45 | 137 | 32.85% |
   | S03 RT Nagar | 20.83 | 37 | 135 | 27.41% |
   | S04 Jayanagar | 21.57 | 34 | 129 | 26.36% |
   | S05 JP Nagar | 21.37 | 39 | 136 | 28.68% |
   | S06 Banashankari | 22.34 | 32 | 143 | 22.38% |
   | S08 Whitefield | 20.63 | 55 | 156 | 35.26% |
   | S09 Indiranagar | 21.08 | 35 | 128 | 27.34% |
   | S10 Marathahalli | 21.74 | 38 | 138 | 27.54% |
   | S11 HSR Layout | 20.91 | 42 | 148 | 28.38% |
   | S13 Malleshwaram | 21.72 | 37 | 136 | 27.21% |
   | S14 Vijayanagar | 21.62 | 50 | 162 | 30.86% |
   | S15 Basaveshwaranagar | 21.97 | 33 | 147 | 22.45% |

4. **Revenue by payment mode and reconciliation gap**

   ```sql
   SELECT payment_mode, delivered_orders, net_revenue
   FROM etl_payment_mode_revenue ORDER BY payment_mode;

   SELECT payment_mode, delivered_orders, net_revenue
   FROM mart_payment_mode_revenue ORDER BY payment_mode;

   SELECT mismatch_reason, COUNT(*) AS mismatches,
          ROUND(SUM(ABS(COALESCE(order_amount, 0)
                        - COALESCE(amount_paid, 0))), 2) AS amount_gap
   FROM etl_payment_reconciliation
   GROUP BY mismatch_reason ORDER BY mismatch_reason;

   SELECT mismatch_reason, COUNT(*) AS mismatches,
          ROUND(SUM(ABS(COALESCE(order_amount, 0)
                        - COALESCE(amount_paid, 0))), 2) AS amount_gap
   FROM mart_payment_reconciliation
   GROUP BY mismatch_reason ORDER BY mismatch_reason;
   ```

   | Payment mode | Delivered orders | Revenue |
   |---|---:|---:|
   | CARD | 379 | 207,312.25 |
   | CASH ON DELIVERY | 255 | 139,186.50 |
   | UPI | 990 | 526,707.65 |
   | WALLET | 213 | 113,561.70 |

   | Reconciliation reason | Mismatches | Amount gap |
   |---|---:|---:|
   | Missing successful payment | 204 | 109,190.35 |
   | Payment without delivered order | 442 | 286,827.53 |
   | **Total** | **646** | **396,017.88** |

5. **Top 10 customers by spend**

   ```sql
   SELECT 'Customer ' || LPAD(CAST(ROW_NUMBER() OVER (
              ORDER BY total_spend DESC, customer_id
          ) AS VARCHAR), 2, '0') AS customer_label,
          loyalty_tier, total_spend
   FROM etl_top_customers ORDER BY total_spend DESC LIMIT 10;

   SELECT 'Customer ' || LPAD(CAST(ROW_NUMBER() OVER (
              ORDER BY total_spend DESC, customer_id
          ) AS VARCHAR), 2, '0') AS customer_label,
          loyalty_tier, total_spend
   FROM mart_top_customers ORDER BY total_spend DESC LIMIT 10;
   ```

   Both queries return the following rank-based aliases; source customer IDs, names, email and phone are omitted from this report:

   | Rank | Customer label | Loyalty tier | Spend |
   |---:|---|---|---:|
   | 1 | Customer 01 | BRONZE | 8,037.70 |
   | 2 | Customer 02 | BRONZE | 7,258.95 |
   | 3 | Customer 03 | PLATINUM | 6,921.20 |
   | 4 | Customer 04 | SILVER | 6,393.95 |
   | 5 | Customer 05 | BRONZE | 6,241.40 |
   | 6 | Customer 06 | BRONZE | 5,877.90 |
   | 7 | Customer 07 | BRONZE | 5,798.45 |
   | 8 | Customer 08 | BRONZE | 5,787.20 |
   | 9 | Customer 09 | GOLD | 5,702.85 |
   | 10 | Customer 10 | BRONZE | 5,693.20 |

6. **Rejected rows by source file and reason**

   ```sql
   SELECT source_file, failure_reason, rejected_rows
   FROM etl_rejected_rows_by_file_reason
   ORDER BY source_file, failure_reason;

   SELECT source_file, failure_reason, rejected_rows
   FROM mart_rejected_rows_by_file_reason
   ORDER BY source_file, failure_reason;
   ```

   The successful run returned 30 ETL file/reason groups and 49 ELT groups. Summed reason incidences by file are:

   | Source file | ETL reason incidences | ELT rejected rows |
   |---|---:|---:|
   | customers.csv | 13 | 8 |
   | orders.csv | 587 | 507 |
   | orders_2026-10-01_increment.csv | 23 | 23 |
   | payments.csv | 43 | 37 |
   | products.csv | 8 | 6 |
   | stores.csv | 2 | 2 |
   | **Total** | **676** | **583** |

   The ETL sum is reason incidences, not unique records: a row can fail multiple checks. The run audit counts **610 unique ETL quarantines** and **583 ELT rejected staging rows**. Row-level reasons and lineage are available in the quarantine CSVs, `dq_issue_details` and `stg_orders`.

## Part E — governance answers

1. **Ownership:** Operations owns order status, delivery and store operations; Finance owns payment settlement and reconciliation; CRM owns customer attributes; Merchandising owns products/costs; Store Operations owns store master data. A cross-functional data owner (Operations + Finance for the 120-minute rule) approves rule changes, with the steward recording version and effective date.
2. **Dictionary:** the curated ETL fact and dimensions are defined below. Source columns retain their source values unless a type/normalization is stated. “Indirect PII” means the key can be linked to a customer or purchase history.

   **`etl_fact_orders`**

   | Column | Type | Meaning / source | Allowed values | PII |
   | `order_id` | VARCHAR | Order key / `orders.order_id` | Nonblank source key | No |
   | `customer_id` | VARCHAR | Customer key / `orders.customer_id` | Existing accepted customer key | Yes, indirect |
   | `product_id` | VARCHAR | Product key / `orders.product_id` | Existing accepted product key | No |
   | `store_id` | VARCHAR | Store key / `orders.store_id` | Existing accepted store key | No |
   | `order_date` | VARCHAR | Local business date / `orders.order_ts` | `YYYY-MM-DD` | No |
   | `order_ts` | TIMESTAMP WITH TIME ZONE | Parsed order timestamp / `orders.order_ts` | Valid timestamp, not future | No |
   | `order_status` | VARCHAR | Normalized order state / `orders.order_status` | `DELIVERED`, `CANCELLED`, `RETURNED` | No |
   | `quantity` | BIGINT | Units ordered / `orders.quantity` | Positive integer | No |
   | `unit_price` | DOUBLE | Pre-discount unit price / `orders.unit_price` | Positive source price | No |
   | `discount_pct` | BIGINT | Discount percentage / `orders.discount_pct` | 0–100 | No |
   | `net_revenue` | DOUBLE | Discounted order amount / `orders.order_amount` | Formula-consistent amount | No |
   | `cost_price` | DOUBLE | Unit cost / `products.cost_price` | Positive catalogue cost | No |
   | `gross_margin` | DOUBLE | Revenue minus quantity × cost | Derived numeric value | No |
   | `payment_mode` | VARCHAR | Normalized mode / `orders.payment_mode` | `UPI`, `CARD`, `CASH ON DELIVERY`, `WALLET` | No |
   | `delivery_minutes` | DOUBLE | Delivery duration / `orders.delivery_minutes` | 0–119 for accepted delivered orders | No |
   | `within_15_min` | BOOLEAN | Derived delivery SLA flag | `TRUE` / `FALSE` | No |
   | `_source_file` | VARCHAR | Source export name | Base order or increment filename | No |
   | `source_row` | BIGINT | Original CSV row number | Positive row number | No |
   | `ingested_at` | TIMESTAMP WITH TIME ZONE | Warehouse load time | Valid timestamp | No |
   | `_run_id` | VARCHAR | Airflow run lineage | Current DAG run identifier | No |

   **Dimensions**

   | Table.column | Type | Meaning / source | Allowed values | PII |
   |---|---|---|---|---|
   | `etl_dim_customer.customer_id` | VARCHAR | Customer key / CRM | Nonblank unique key | Yes, indirect |
   | `etl_dim_customer.email_masked` | VARCHAR | Masked email / CRM `email` | Masked string or `***` | Yes, masked |
   | `etl_dim_customer.phone_masked` | VARCHAR | Masked phone / CRM `phone` | Masked string or `***` | Yes, masked |
   | `etl_dim_customer.loyalty_tier` | VARCHAR | Normalized tier / CRM | `BRONZE`, `SILVER`, `GOLD`, `PLATINUM` | Yes, indirect |
   | `etl_dim_customer.is_active` | VARCHAR | Active flag / CRM | Source flag (`TRUE`/`FALSE`) | Yes, indirect |
   | `etl_dim_product.product_id` | VARCHAR | Product key / catalogue | Nonblank unique key | No |
   | `etl_dim_product.product_name` | VARCHAR | Display name / catalogue | Source catalogue value | No |
   | `etl_dim_product.category` | VARCHAR | Reporting category / catalogue | Dairy & Bakery; Fruits & Vegetables; Household; Personal Care; Snacks & Beverages; Staples | No |
   | `etl_dim_product.sub_category` | VARCHAR | Product subcategory / catalogue | Source-controlled product subcategories | No |
   | `etl_dim_product.brand` | VARCHAR | Brand / catalogue | Source-controlled catalogue brands | No |
   | `etl_dim_product.cost_price` | DOUBLE | Unit cost / catalogue | Positive numeric value | No |
   | `etl_dim_product.mrp` | VARCHAR | Listed retail price / catalogue | Positive numeric source text | No |
   | `etl_dim_product.unit` | VARCHAR | Sale unit / catalogue | Source-controlled units | No |
   | `etl_dim_product.is_perishable` | VARCHAR | Perishability flag / catalogue | Source flag (`TRUE`/`FALSE`) | No |
   | `etl_dim_store.store_id` | VARCHAR | Store key / store master | Nonblank unique key | No |
   | `etl_dim_store.store_name` | VARCHAR | Store display name / store master | Source master value | No |
   | `etl_dim_store.zone` | VARCHAR | Normalized business zone / store master | East, North, South, West | No |
   | `etl_dim_store.area` | VARCHAR | Store area / store master | Source master value | No |
   | `etl_dim_store.pincode` | VARCHAR | Store postal code / store master | Six-digit string when valid | No |
   | `etl_dim_store.capacity_orders_per_day` | BIGINT | Daily capacity / store master | Positive integer | No |

   The customer dimension deliberately omits raw name, email, phone and customer address; the reporting contact fields remain masked and are still treated as personal data. ELT raw tables preserve every source column as VARCHAR, plus load/run metadata; raw customer fields are personal data and require restricted access.
3. **Lineage:** `etl_daily_zone_revenue` for 2026-09-01, East, S08 (NammaMart Whitefield) is 2,984.70 from `orders.csv` rows 1050 (ORD000008, 2,060.00), 229 (ORD000051, 73.60), 193 (ORD000056, 39.90) and 1351 (ORD000079, 811.20). ETL facts store source file and row; ELT can additionally recover the exact raw text and run metadata, so ELT is easier to audit.
4. **Personal data:** direct personal identifiers include `customers.full_name`, `email`, `phone` and `stores.manager_name`; customer ID, address/pincode, signup date and purchase history are linkable and should also be access controlled. Reporting masks email/phone and omits names/manager names; masking preserves limited contact context for authorized operations without exposing the raw value. India's Digital Personal Data Protection Act, 2023 applies to digital personal data and expects lawful purpose/notice, appropriate safeguards and data-principal rights; this demo is not a legal compliance certification.
5. **Access control:** raw tables and raw-customer PII are restricted to named data/platform administrators for replay and audit. Data engineering and quality stewards may access masked staging/quarantine records needed to remediate; Finance and Operations consume marts. Raw phone/email should not be exposed to analysts or store managers.
6. **Retention:** proposed policy: raw exports 365 days, quarantine 90 days after resolution, masked customer reporting data for the approved finance/operations retention period. After expiry, securely delete raw/quarantine data and remove or aggregate customer-level data; retention must be approved against statutory and finance requirements before production.
7. **Secrets:** store credentials such as `SLACK_WEBHOOK_URL` in Airflow Variables or a secrets backend and retrieve through `Variable.get()`. No webhook or API credential is present in these DAGs; secrets must not enter source control, logs or the raw/profile files.
8. **Rejected-order audit:** the controlled test row `EXP_BAD_NEGATIVE_QUANTITY` was read from the ETL quarantine file at `orders.csv` source row 2415. Its received values included quantity `-1`, unit price `222`, discount `5`, and order amount `-210.90`; the recorded reason was `quantity must be a positive number`. ELT's `raw_orders` retains that exact row and run, and `stg_orders` marks it `FAIL` with reason `invalid quantity`. The ETL fact count for that ID is zero.

   ```sql
   SELECT order_id, quantity, unit_price, discount_pct, order_amount,
          _source_file, _source_row, _run_id
   FROM raw_orders
   WHERE order_id = 'EXP_BAD_NEGATIVE_QUANTITY'
     AND _run_id = 'manual__compliance_five_bad_rows_elt_aaf79391';

   SELECT order_id, dq_status, failure_reason
   FROM stg_orders
   WHERE order_id = 'EXP_BAD_NEGATIVE_QUANTITY'
     AND _run_id = 'manual__compliance_five_bad_rows_elt_aaf79391';

   SELECT COUNT(*) AS finance_fact_rows
   FROM etl_fact_orders
   WHERE order_id = 'EXP_BAD_NEGATIVE_QUANTITY';
   ```

   The observed results are the original raw values above, `FAIL / invalid quantity`, and `finance_fact_rows = 0`. See the sanitized [quarantine sample screenshot](submission_assets/airflow/quarantine_sample.png).
9. **Change history:** both current-state reporting paths apply the October increment before calculating September revenue, so returned corrections no longer count as delivered revenue; the verified current September total is 918,754.95 in each pipeline. Finance should use the corrected close, while preserving prior close snapshots or an effective-dated status history; ELT's append-only raw history can reconstruct prior state, whereas the ETL fact intentionally holds only the current valid snapshot.

## Part F — Airflow observability experiments

All ten experiments were run on both DAGs in the local Astro Airflow runtime on 2026-10-21. Every controlled source mutation was restored; the six input-file hashes matched their pre-experiment backups. The corrected distribution-drift rerun used the same pandas `.round(2)` formula as ETL. The two recovery actions below were performed with Airflow UI **Clear**.

| # | Expected | Observed | Where verified | What I would change |
|---:|---|---|---|---|
| 1 Happy path | Parallel extracts, validation, load, reports and audit all succeed. | Both runs succeeded. ETL extracted 5,212 rows, loaded 1,889 facts and quarantined 587 rows. ELT extracted/loaded 5,212 raw rows and recorded 560 rejected rows. | Runs `manual__compliance_happy_path_etl_7232f859` / `manual__compliance_happy_path_elt_ae8b1ea0`; Airflow graph, `pipeline_runs`, `dq_results`. | Keep source arrival checks and production alerting before the 09:00 SLA. |
| 2 Re-run / idempotency | Reprocessing the same inputs does not double-count finance facts; ELT preserves raw history but keeps one current order per key. | Both reruns succeeded. ETL fact count remained stable. ELT `raw_orders` grew from 20,626 to 23,038 (**+2,412 base rows**) while staging stayed at 2,550 unique order IDs including the increment. | Runs `manual__compliance_idempotency_etl_f094314e` / `manual__compliance_idempotency_elt_ac5b626f`; before/after warehouse counts. | Apply an approved raw-history retention policy and monitor storage growth. |
| 3 Incremental load | Apply 150 new orders and 10 corrections without duplicate current keys. | The increment contained 160 rows: ELT staged 150 new IDs and updated 10 existing IDs. ETL accepted 131 new and 6 corrected facts; 23 rows referencing rejected/ambiguous master keys were quarantined. Both runs succeeded. | Runs `manual__compliance_incremental_etl_397c25a8` / `manual__compliance_incremental_elt_3e546190`; staged keys and ETL fact lineage. | Confirm the handling policy for the 23 rejected master-key references with the client. |
| 4 Missing source file | Missing `products.csv` retries, then fails loudly and creates a failed audit row. | `extract_products` failed after 3 attempts (2 retries); other extracts succeeded, downstream tasks became upstream-failed, and `audit_run` failed. The DAG run failed with 5,220 extracted, 0 loaded and 0 quarantined rows. Elapsed run time was about 4m14s per DAG. | Runs `manual__compliance_missing_products_etl_a8ac5909` / `manual__compliance_missing_products_elt_8d34d66b`; Airflow task table/logs and `pipeline_runs`. | Add an alert that includes the failed source and task; preserve the two-retry policy. |
| 5 Five bad orders | Null customer, negative quantity, unknown store, `SHIPPED` status and future timestamp are each rejected with reasons. | Added five `EXP_BAD_*` rows. Each appeared in ETL quarantine and ELT staging with `dq_status='FAIL'`; audit quarantine counts rose by 5 (ETL 610→615; ELT 583→588). Targeted `dq_results` checks showed FAIL, including quantity 11, missing customer 19, future order 6, and invalid status 4 for ETL; those totals include pre-existing bad records. | Runs `manual__compliance_five_bad_rows_etl_df75f310` / `manual__compliance_five_bad_rows_elt_aaf79391`; quarantine CSV, `dq_issue_details`, `raw_orders`, `stg_orders`, `dq_results`. | Route critical rejects to steward remediation; do not treat the total check failures as only the five injected rows. |
| 6 Schema change | Renaming `order_amount` to `total` fails before an incorrect load. | `extract_orders` failed after 3 attempts for both DAGs. Each audit row was FAILED with 2,960 rows extracted and zero loaded/quarantined; no silent schema adaptation occurred. | Runs `manual__compliance_schema_change_total_etl_3093daad` / `manual__compliance_schema_change_total_elt_95ef0de0`; Airflow extract logs and `pipeline_runs`. | Version and approve source schemas; notify the owning source team when the header contract changes. |
| 7 Volume anomaly | A 50-row snapshot, under 10% of the previous full snapshot, raises a warning. | `orders_volume_anomaly` recorded 50 checked / 50 failed with WARN in both DAGs; both runs succeeded. ETL audit: 3,010 extracted, 175 loaded, 2,230 quarantined. ELT audit: 3,010 extracted/loaded, 87 rejected. The ETL quarantine total also includes consistency failures caused by the incomplete order snapshot. | Runs `manual__compliance_volume_50_etl_1a97d23f` / `manual__compliance_volume_50_elt_9d8fc612`; `dq_results` and `pipeline_runs`. | Make the volume threshold configurable only after business approval; page on unexpected drops. |
| 8 Distribution drift | Multiplying every `S08` order's `unit_price` by 100 produces a distribution warning and visible mart movement. | Changed all 177 `S08` rows and recalculated `order_amount` with the ETL pandas formula. Both runs succeeded. ETL warned on 169/2,412 rows; ELT warned on 177/2,572. Category revenue moved 986,768.10→7,717,877.55 (**+6,731,109.45**); gross margin moved 230,069.81→6,960,951.11 (**+6,730,881.30**) in both. Because payment exports were unchanged, payment-amount mismatches also rose by 147 and added 147 quarantined payment rows. | Corrected runs `manual__distribution_drift_corrected_etl_1792590684_a334` / `manual__distribution_drift_corrected_elt_1792590684_a268`; `dq_results`, category marts and audit counts. | Quarantine or hold finance publication on a distribution warning and reconcile affected payments before release. |
| 9 Freshness | Orders whose latest timestamp is 30 days old trigger a stale-data warning. | Set base and increment timestamps to 2026-09-21 12:00. ETL warned on all 2,412 base orders and all 160 increment rows; ELT warned on all 2,412 base-snapshot rows. Both DAG runs succeeded; order future-timestamp checks passed. | Runs `manual__compliance_freshness_30_days_etl_bfc789b5` / `manual__compliance_freshness_30_days_elt_af9418d2`; `dq_results` timeliness rows and `pipeline_runs`. | Add a freshness gate/notification before marts are served when the source is stale. |
| 10 Recovery with UI Clear | After restoring the source, Clear reruns the failed extract and downstream tasks only. | In the UI, cleared failed `extract_products` and its four downstream tasks for both runs. ETL reran those five tasks; ELT did the same. Each `extract_products` succeeded on try 4, `audit_run` on try 2, all downstream tasks succeeded on try 1, and the other five parallel extracts remained on try 1. Both DAG runs and audit rows became SUCCESS (ETL 5,372/2,020/610; ELT 5,372/5,372/583 extracted/loaded/quarantined). | Recovered runs `manual__compliance_missing_products_etl_a8ac5909` / `manual__compliance_missing_products_elt_8d34d66b`; Airflow Clear dialog, task instances and `pipeline_runs`. | Keep task-level recovery scoped to the failed node and downstream work; the observed upstream extracts correctly remained untouched. |

Observability pillars covered: **freshness** (stale/future timestamps), **volume** (prior-snapshot comparison), **schema** (exact header validation), **distribution** (price versus prior accepted product median), and **lineage** (source file, row and run metadata). Local Airflow task state, retries, DQ outcomes and recovery were observed. The strongest coverage is row-level quality/lineage; the weakest area is external incident notification and production scheduler availability, neither of which was exercised locally.

The restored-input recovery runs are the final clean baseline: ETL and ELT both succeeded and September delivered revenue is **918,754.95** in each. ELT staging contains 2,550 unique order IDs. The ignored local DuckDB warehouse retains raw customer data and experiment history; do not publish it. Evidence: [ETL green graph](submission_assets/airflow/etl_graph.png), [ELT green graph](submission_assets/airflow/elt_graph.png), [ETL schedule](submission_assets/airflow/etl_schedule.png), [ELT schedule](submission_assets/airflow/elt_schedule.png), [sanitized quarantine sample](submission_assets/airflow/quarantine_sample.png), [DQ results](submission_assets/airflow/dq_results.png), [pipeline runs](submission_assets/airflow/pipeline_runs.png), and [payment reconciliation](submission_assets/airflow/payment_reconciliation.png).

## Part G — recommendation to Sameer (CTO)

**Recommendation: adopt ELT with a strict, quality-gated mart contract.** The restored local runs retained all 5,372 source rows in ELT, applied the 150 inserts and 10 corrections into 2,550 unique staged order IDs, and matched ETL's September delivered revenue of 918,754.95. ELT provides audit/replay history without allowing failed staging rows into finance marts. Keep raw customer PII restricted, and allow Finance to query only validated marts.

ETL offers a strong “bad records never enter finance tables” boundary and is simple for the current 2,400-order scale. ELT makes rule changes, lineage and historical correction analysis easier because the raw source is preserved; the cost is more storage and a larger access-control burden. At 1,000× volume, benchmark SQL scans/merges and file retention before scaling; DuckDB is a useful single-node/open-source warehouse, but sustained concurrent writers or multi-team serving would justify partitioned Parquet plus a centrally managed open-source analytical warehouse. The shared-file writer lock serializes these two local DAGs, not a distributed production architecture.

Top three risks: (1) raw PII exposure or retention beyond approved purpose; (2) a bad or unreviewed quality rule excluding valid orders or allowing bad data into a mart; (3) single-machine/Docker/DuckDB availability and concurrency limits jeopardizing the 09:00 delivery SLA. The local acceptance experiments, recovery and evidence capture are complete. Before production sign-off, configure access control, approved retention, external failure notifications and an always-on scheduler, then validate the 09:00 delivery SLA in the deployment environment.
