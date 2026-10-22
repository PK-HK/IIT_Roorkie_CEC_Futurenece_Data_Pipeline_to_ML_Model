"""Daily NammaMart ETL pipeline: validate before loading finance marts."""

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
    build_etl_reports,
    extract_source,
    get_pipeline_run_counts,
    transform_load_etl,
    validate_etl_sources,
    write_run_audit,
)


DAG_ID = "nammamart_etl_pipeline"
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
    tags=["nammamart", "etl", "finance", "daily"],
    doc_md=__doc__,
)
def nammamart_etl_pipeline():
    @task
    def extract_customers():
        return extract_source("customers")

    @task
    def extract_products():
        return extract_source("products")

    @task
    def extract_stores():
        return extract_source("stores")

    @task
    def extract_orders():
        return extract_source("orders")

    @task
    def extract_payments():
        return extract_source("payments")

    @task
    def extract_increment():
        from nammamart_pipeline import INCREMENT_FILE

        if not (DATA_DIR / INCREMENT_FILE).exists():
            print("[Extract] Increment file is not present; continuing with full snapshot.")
            return {"file": INCREMENT_FILE, "rows": 0}
        return extract_source("orders", increment=True)

    @task
    def validate_sources(*extract_results, **context):
        if not all(extract_results):
            raise ValueError("One or more source extract tasks returned no result.")
        return validate_etl_sources(
            run_id=context["run_id"],
            dag_id=context["dag"].dag_id,
        )

    @task
    def transform_and_load(validation, **context):
        return transform_load_etl(validation, context["run_id"])

    @task
    def reconcile_and_report(validation, load_counts, **context):
        report_counts = build_etl_reports(context["run_id"], context["dag"].dag_id)
        return {
            "rows_extracted": validation["rows_extracted"],
            "rows_quarantined": validation["rows_quarantined"],
            "rows_loaded": load_counts["rows_loaded"],
            **report_counts,
        }

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run(**context):
        task_ids = (
            "extract_customers", "extract_products", "extract_stores",
            "extract_orders", "extract_payments", "extract_increment",
            "validate_sources", "transform_and_load", "reconcile_and_report",
        )
        failed = any(
            context["ti"].xcom_pull(task_ids=task_id) is None
            for task_id in task_ids
        )
        validation = context["ti"].xcom_pull(task_ids="validate_sources") or {}
        loaded = context["ti"].xcom_pull(task_ids="transform_and_load") or {}
        extracted = validation.get("rows_extracted") or get_pipeline_run_counts(
            [
                "extract_customers", "extract_products", "extract_stores",
                "extract_orders", "extract_payments", "extract_increment",
            ],
            context["ti"],
        )
        quarantined = validation.get("rows_quarantined", 0)
        rows_loaded = loaded.get("rows_loaded", 0)
        write_run_audit(
            run_id=context["run_id"],
            dag_id=context["dag"].dag_id,
            start_time=context["dag_run"].start_date,
            rows_extracted=extracted,
            rows_loaded=rows_loaded,
            rows_quarantined=quarantined,
            status="FAILED" if failed else "SUCCESS",
        )
        if failed:
            raise AirflowFailException(
                "One or more upstream tasks failed; the run audit was recorded as FAILED."
            )

    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()
    orders = extract_orders()
    payments = extract_payments()
    increment = extract_increment()
    validation = validate_sources(customers, products, stores, orders, payments, increment)
    loaded = transform_and_load(validation)
    report = reconcile_and_report(validation, loaded)
    audit = audit_run()
    audit.set_upstream([
        customers, products, stores, orders, payments, increment,
        validation, loaded, report,
    ])


nammamart_etl_pipeline()
