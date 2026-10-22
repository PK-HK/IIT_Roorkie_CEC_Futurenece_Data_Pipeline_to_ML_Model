"""Daily NammaMart ELT pipeline: preserve raw exports and transform in DuckDB."""

import os
import sys
from datetime import timedelta

sys.path.insert(0, "/usr/local/airflow/include")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "include")))

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, task
from airflow.task.trigger_rule import TriggerRule

from nammamart_pipeline import (
    DATA_DIR,
    INCREMENT_FILE,
    build_elt_reports,
    extract_source,
    get_pipeline_run_counts,
    get_staging_rejected_count,
    load_raw_tables,
    run_sql_layer,
    write_run_audit,
)


DAG_ID = "nammamart_elt_pipeline"
DEFAULT_ARGS = {
    "owner": "nammamart-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=45),
}


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "elt", "audit", "daily"],
    doc_md=__doc__,
)
def nammamart_elt_pipeline():
    @task
    def extract_source_file(name: str):
        return extract_source(name)

    @task
    def extract_increment():
        if not (DATA_DIR / INCREMENT_FILE).exists():
            print("[Extract] Increment file is not present.")
            return {"file": INCREMENT_FILE, "rows": 0}
        return extract_source("orders", increment=True)

    @task
    def load_raw(*extract_results, **context):
        if not all(extract_results):
            raise ValueError("One or more source extract tasks returned no result.")
        return load_raw_tables(context["run_id"])

    @task
    def transform_staging(raw_counts, **context):
        run_sql_layer(
            "nammamart_staging.sql",
            run_id=context["run_id"],
            dag_id=context["dag"].dag_id,
        )
        return raw_counts

    @task
    def transform_marts(raw_counts, **context):
        marts = build_elt_reports(
            run_id=context["run_id"], dag_id=context["dag"].dag_id
        )
        return {
            "rows_extracted": sum(raw_counts.values()),
            "rows_loaded": sum(raw_counts.values()),
            "marts": marts,
        }

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run(**context):
        task_ids = (
            "extract_customers", "extract_products", "extract_stores",
            "extract_orders", "extract_payments", "extract_increment",
            "load_raw", "transform_staging", "transform_marts",
        )
        failed = any(
            context["ti"].xcom_pull(task_ids=task_id) is None
            for task_id in task_ids
        )
        raw_counts = context["ti"].xcom_pull(task_ids="load_raw") or {}
        results = context["ti"].xcom_pull(task_ids="transform_marts") or {}
        rows_extracted = sum(raw_counts.values()) or get_pipeline_run_counts(
            [
                "extract_customers", "extract_products", "extract_stores",
                "extract_orders", "extract_payments", "extract_increment",
            ],
            context["ti"],
        )
        rows_quarantined = get_staging_rejected_count(context["run_id"])
        write_run_audit(
            run_id=context["run_id"],
            dag_id=context["dag"].dag_id,
            start_time=context["dag_run"].start_date,
            rows_extracted=rows_extracted,
            rows_loaded=results.get("rows_loaded", 0),
            rows_quarantined=rows_quarantined,
            status="FAILED" if failed else "SUCCESS",
        )
        if failed:
            raise AirflowFailException(
                "One or more upstream tasks failed; the run audit was recorded as FAILED."
            )

    customers = extract_source_file.override(task_id="extract_customers")("customers")
    products = extract_source_file.override(task_id="extract_products")("products")
    stores = extract_source_file.override(task_id="extract_stores")("stores")
    orders = extract_source_file.override(task_id="extract_orders")("orders")
    payments = extract_source_file.override(task_id="extract_payments")("payments")
    increment = extract_increment()
    loaded = load_raw(customers, products, stores, orders, payments, increment)
    staged = transform_staging(loaded)
    marts = transform_marts(staged)
    audit = audit_run()
    audit.set_upstream([
        customers, products, stores, orders, payments, increment,
        loaded, staged, marts,
    ])


nammamart_elt_pipeline()
