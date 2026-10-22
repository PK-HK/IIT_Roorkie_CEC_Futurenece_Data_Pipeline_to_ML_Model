import contextlib
import csv
import io
import shutil
import tempfile
import unittest
from pathlib import Path
import sys

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
import nammamart_pipeline as pipeline
import profile_nammamart as profile_generator


class NammaMartPipelineTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="nammamart-test-")
        self.original_paths = (
            pipeline.WAREHOUSE_DB,
            pipeline.PROFILE_DIR,
            pipeline.QUARANTINE_DIR,
        )
        root = Path(self.temp_dir.name)
        pipeline.WAREHOUSE_DB = root / "warehouse.duckdb"
        pipeline.PROFILE_DIR = root / "profile"
        pipeline.QUARANTINE_DIR = root / "quarantine"

    def tearDown(self):
        (
            pipeline.WAREHOUSE_DB,
            pipeline.PROFILE_DIR,
            pipeline.QUARANTINE_DIR,
        ) = self.original_paths
        self.temp_dir.cleanup()

    def test_etl_and_elt_contracts(self):
        run_id = "test-run-1"
        with contextlib.redirect_stdout(io.StringIO()):
            validation = pipeline.validate_etl_sources(
                run_id, "nammamart_etl_pipeline"
            )
            load_result = pipeline.transform_load_etl(validation, run_id)
            pipeline.build_etl_reports(run_id, "nammamart_etl_pipeline")
            pipeline.load_raw_tables(run_id)
            pipeline.run_sql_layer(
                "nammamart_staging.sql", run_id, "nammamart_elt_pipeline"
            )
            marts = pipeline.build_elt_reports(run_id, "nammamart_elt_pipeline")

        with duckdb.connect(str(pipeline.WAREHOUSE_DB), read_only=True) as con:
            fact_rows, fact_ids = con.execute(
                "SELECT COUNT(*), COUNT(DISTINCT order_id) FROM etl_fact_orders"
            ).fetchone()
            delivered_rows = con.execute(
                "SELECT COUNT(*) FROM etl_fact_orders WHERE order_status='DELIVERED'"
            ).fetchone()[0]
            staged_rows, staged_pass = con.execute(
                "SELECT COUNT(*), COUNT(*) FILTER (WHERE dq_status <> 'FAIL') "
                "FROM stg_orders"
            ).fetchone()
            september_etl = con.execute("""
                SELECT ROUND(SUM(net_revenue), 2)
                FROM etl_fact_orders
                WHERE order_status='DELIVERED'
                  AND order_date BETWEEN '2026-09-01' AND '2026-09-30'
            """).fetchone()[0]
            september_elt = con.execute("""
                SELECT ROUND(SUM(net_revenue), 2)
                FROM mart_daily_store_kpis
                WHERE order_date BETWEEN '2026-09-01' AND '2026-09-30'
            """).fetchone()[0]
            raw_orders = con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
            increment_rows, increment_new, increment_updated = con.execute("""
                SELECT COUNT(*),
                       COUNT(*) FILTER (WHERE NOT EXISTS (
                           SELECT 1 FROM raw_orders base
                           WHERE base._run_id = ?
                             AND base._source_file = 'orders.csv'
                             AND base.order_id = stage.order_id
                       )),
                       COUNT(*) FILTER (WHERE EXISTS (
                           SELECT 1 FROM raw_orders base
                           WHERE base._run_id = ?
                             AND base._source_file = 'orders.csv'
                             AND base.order_id = stage.order_id
                       ))
                FROM stg_orders stage
                WHERE _source_file = 'orders_2026-10-01_increment.csv'
            """, [run_id, run_id]).fetchone()
            etl_recon = con.execute(
                "SELECT COUNT(*) FROM etl_payment_reconciliation"
            ).fetchone()[0]
            elt_recon = con.execute(
                "SELECT COUNT(*) FROM mart_payment_reconciliation"
            ).fetchone()[0]
            pii_columns = {
                row[0] for row in con.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name IN ('etl_dim_customer', 'stg_customers')
                """).fetchall()
            }
            dimensions = {
                row[0] for row in con.execute(
                    "SELECT DISTINCT dimension FROM dq_results WHERE run_id=?",
                    [run_id],
                ).fetchall()
            }
            dq_counts = dict(con.execute(
                "SELECT dag_id, COUNT(*) FROM dq_results WHERE run_id=? GROUP BY dag_id",
                [run_id],
            ).fetchall())

        self.assertEqual(fact_rows, fact_ids)
        self.assertEqual(2020, fact_rows)
        self.assertEqual(1837, delivered_rows)
        self.assertEqual(fact_rows, staged_pass)
        self.assertEqual(2572, raw_orders)
        self.assertEqual((160, 150, 10), (increment_rows, increment_new, increment_updated))
        self.assertEqual(september_etl, september_elt)
        self.assertEqual(646, etl_recon)
        self.assertEqual(etl_recon, elt_recon)
        self.assertEqual({
            "nammamart_etl_pipeline": 68,
            "nammamart_elt_pipeline": 34,
        }, dq_counts)
        self.assertFalse({"full_name", "email", "phone"} & pii_columns)
        self.assertTrue({
            "completeness", "uniqueness", "validity", "accuracy",
            "consistency", "timeliness",
        }.issubset(dimensions))
        self.assertGreater(marts["mart_payment_mode_revenue"], 0)
        self.assertGreater(validation["rows_quarantined"], 0)
        self.assertGreater(load_result["rows_loaded"], 0)
        clean_customer_file = Path(validation["accepted_paths"]["customers"])
        with clean_customer_file.open(encoding="utf-8") as source:
            customer_columns = set(source.readline().strip().split(","))
        self.assertFalse(
            {"full_name", "email", "phone", "city", "area", "pincode"}
            & customer_columns
        )
        issue_log = pipeline.PROFILE_DIR / "nammamart_data_issues.csv"
        with issue_log.open(encoding="utf-8", newline="") as source:
            issue_rows = list(csv.DictReader(source))
        issue_columns = set(issue_rows[0]) if issue_rows else set()
        self.assertTrue({"row_key", "source_row", "column", "problem"} <= issue_columns)
        with (pipeline.DATA_DIR / "customers.csv").open(
            encoding="utf-8", newline=""
        ) as source:
            customer_ids = {
                row["customer_id"] for row in csv.DictReader(source)
            }
        customer_issues = [
            row for row in issue_rows if row["file"] == "customers.csv"
        ]
        self.assertTrue(customer_issues)
        self.assertTrue(all(
            row["row_key"] == f"customers.csv:row_{row['source_row']}"
            for row in customer_issues
        ))
        self.assertFalse(any(
            customer_id in row["row_key"]
            for customer_id in customer_ids
            for row in customer_issues
        ))

    def test_profile_redacts_duplicate_customer_ids(self):
        with (pipeline.DATA_DIR / "customers.csv").open(
            encoding="utf-8", newline=""
        ) as source:
            customer_ids = {
                row["customer_id"] for row in csv.DictReader(source)
            }

        customer_profile = profile_generator.build_profile()["files"]["customers"]

        self.assertTrue(customer_profile["duplicate_key_values_redacted"])
        self.assertEqual(5, len(customer_profile["duplicate_key_values"]))
        self.assertFalse(
            customer_ids.intersection(customer_profile["duplicate_key_values"])
        )

        with contextlib.redirect_stdout(io.StringIO()):
            pipeline.transform_load_etl(validation, run_id)
            pipeline.load_raw_tables("test-run-2")
            pipeline.run_sql_layer(
                "nammamart_staging.sql", "test-run-2", "nammamart_elt_pipeline"
            )
            pipeline.build_elt_reports("test-run-2", "nammamart_elt_pipeline")
        with duckdb.connect(str(pipeline.WAREHOUSE_DB), read_only=True) as con:
            self.assertEqual(
                fact_rows,
                con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0],
            )
            self.assertEqual(
                staged_rows,
                con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0],
            )
            self.assertEqual(
                5144,
                con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0],
            )
            expected_run_2_rejected = sum(
                con.execute(
                    f"SELECT COUNT(*) FROM {table} "
                    "WHERE dq_status='FAIL' AND _run_id=?",
                    ["test-run-2"],
                ).fetchone()[0]
                for table in (
                    "stg_orders",
                    "stg_payments",
                    "stg_customers",
                    "stg_products",
                    "stg_stores",
                )
            )
            mart_rejected = con.execute(
                "SELECT COALESCE(SUM(rejected_rows), 0) "
                "FROM mart_rejected_rows_by_file_reason"
            ).fetchone()[0]
        self.assertEqual(expected_run_2_rejected, mart_rejected)
        self.assertEqual(0, pipeline.get_staging_rejected_count(run_id))
        self.assertGreater(expected_run_2_rejected, 0)
        self.assertEqual(
            expected_run_2_rejected,
            pipeline.get_staging_rejected_count("test-run-2"),
        )
        with duckdb.connect(str(pipeline.WAREHOUSE_DB)) as con:
            con.execute("DROP TABLE stg_stores")
        with self.assertRaisesRegex(RuntimeError, "stg_stores"):
            pipeline.get_staging_rejected_count("test-run-2")
        with duckdb.connect(str(pipeline.WAREHOUSE_DB)) as con:
            for table in (
                "stg_orders",
                "stg_payments",
                "stg_customers",
                "stg_products",
            ):
                con.execute(f"DROP TABLE {table}")
        self.assertEqual(0, pipeline.get_staging_rejected_count("test-run-2"))

    def test_etl_volume_and_price_drift_warnings(self):
        original_data_dir = pipeline.DATA_DIR
        test_data_dir = Path(self.temp_dir.name) / "data"
        shutil.copytree(original_data_dir, test_data_dir)
        pipeline.DATA_DIR = test_data_dir
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                baseline = pipeline.validate_etl_sources(
                    "baseline-run", "nammamart_etl_pipeline"
                )
                pipeline.transform_load_etl(baseline, "baseline-run")

                orders_path = test_data_dir / "orders.csv"
                with orders_path.open(encoding="utf-8", newline="") as source:
                    reader = csv.DictReader(source)
                    fieldnames = reader.fieldnames
                    orders = list(reader)
                with orders_path.open("w", encoding="utf-8", newline="") as target:
                    writer = csv.DictWriter(target, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(orders[:50])

                pipeline.validate_etl_sources(
                    "volume-run", "nammamart_etl_pipeline"
                )

            with duckdb.connect(str(pipeline.WAREHOUSE_DB), read_only=True) as con:
                volume = con.execute(
                    "SELECT rows_checked, rows_failed, status FROM dq_results "
                    "WHERE run_id='volume-run' AND check_name='orders_volume_anomaly'"
                ).fetchone()
                baseline_medians = dict(con.execute(
                    "SELECT product_id, MEDIAN(unit_price) "
                    "FROM etl_fact_orders WHERE unit_price > 0 GROUP BY product_id"
                ).fetchall())
            self.assertEqual((50, 50, "WARN"), volume)

            with orders_path.open("w", encoding="utf-8", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(orders[:50])
            with contextlib.redirect_stdout(io.StringIO()):
                pipeline.validate_etl_sources(
                    "volume-run-again", "nammamart_etl_pipeline"
                )
            with duckdb.connect(str(pipeline.WAREHOUSE_DB), read_only=True) as con:
                repeated_volume = con.execute(
                    "SELECT rows_checked, rows_failed, status FROM dq_results "
                    "WHERE run_id='volume-run-again' "
                    "AND check_name='orders_volume_anomaly'"
                ).fetchone()
            self.assertEqual((50, 50, "WARN"), repeated_volume)

            with orders_path.open("w", encoding="utf-8", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(orders)
            store_id = orders[0]["store_id"]
            expected_drift_rows = 0
            for order in orders:
                if order["store_id"] != store_id:
                    continue
                order["unit_price"] = str(float(order["unit_price"]) * 100)
                median = baseline_medians.get(order["product_id"])
                if median is not None and float(order["unit_price"]) > 5 * median:
                    expected_drift_rows += 1
                order["order_amount"] = str(round(
                    float(order["quantity"]) * float(order["unit_price"])
                    * (1 - float(order["discount_pct"]) / 100),
                    2,
                ))
            with orders_path.open("w", encoding="utf-8", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(orders)

            with contextlib.redirect_stdout(io.StringIO()):
                pipeline.validate_etl_sources(
                    "drift-run", "nammamart_etl_pipeline"
                )
            with duckdb.connect(str(pipeline.WAREHOUSE_DB), read_only=True) as con:
                drift = con.execute(
                    "SELECT rows_checked, rows_failed, status FROM dq_results "
                    "WHERE run_id='drift-run' "
                    "AND check_name='orders_distribution_unit_price'"
                ).fetchone()
            self.assertEqual((2412, expected_drift_rows, "WARN"), drift)
        finally:
            pipeline.DATA_DIR = original_data_dir

    def test_elt_volume_warning_uses_last_full_snapshot(self):
        original_data_dir = pipeline.DATA_DIR
        test_data_dir = Path(self.temp_dir.name) / "elt-data"
        shutil.copytree(original_data_dir, test_data_dir)
        pipeline.DATA_DIR = test_data_dir
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                pipeline.load_raw_tables("elt-full-run")
                pipeline.run_sql_layer(
                    "nammamart_staging.sql",
                    "elt-full-run",
                    "nammamart_elt_pipeline",
                )
            orders_path = test_data_dir / "orders.csv"
            with orders_path.open(encoding="utf-8", newline="") as source:
                reader = csv.DictReader(source)
                fields = reader.fieldnames
                full_orders = list(reader)
            with orders_path.open("w", encoding="utf-8", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=fields)
                writer.writeheader()
                writer.writerows(full_orders[:50])

            for run_id in ("elt-volume-run", "elt-volume-run-again"):
                with contextlib.redirect_stdout(io.StringIO()):
                    pipeline.load_raw_tables(run_id)
                    pipeline.run_sql_layer(
                        "nammamart_staging.sql",
                        run_id,
                        "nammamart_elt_pipeline",
                    )
            with duckdb.connect(str(pipeline.WAREHOUSE_DB), read_only=True) as con:
                results = con.execute(
                    "SELECT run_id, rows_checked, rows_failed, status "
                    "FROM dq_results WHERE check_name='orders_volume_anomaly' "
                    "AND run_id IN ('elt-volume-run','elt-volume-run-again') "
                    "ORDER BY run_id"
                ).fetchall()
            self.assertEqual([
                ("elt-volume-run", 50, 50, "WARN"),
                ("elt-volume-run-again", 50, 50, "WARN"),
            ], results)
        finally:
            pipeline.DATA_DIR = original_data_dir


if __name__ == "__main__":
    unittest.main()
