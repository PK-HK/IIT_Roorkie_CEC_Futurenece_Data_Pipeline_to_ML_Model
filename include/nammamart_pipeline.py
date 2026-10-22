"""Shared NammaMart ETL/ELT utilities and data-quality rules."""

from __future__ import annotations

import csv
import importlib
import os
import re
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable, ParamSpec, TypeVar

import duckdb
import pandas as pd


INCLUDE_DIR = Path(__file__).resolve().parent
DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
WAREHOUSE_DB = INCLUDE_DIR / "warehouse_nammamart_pipeline.duckdb"
QUARANTINE_DIR = INCLUDE_DIR / "quarantine" / "nammamart"
PROFILE_DIR = INCLUDE_DIR / "profile"
SQL_DIR = INCLUDE_DIR / "sql"

SOURCE_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}
EXPECTED_HEADERS = {
    "orders": [
        "order_id", "order_ts", "customer_id", "store_id", "product_id",
        "quantity", "unit_price", "discount_pct", "order_amount",
        "payment_mode", "order_status", "channel", "delivery_minutes",
    ],
    "payments": [
        "payment_id", "order_id", "payment_ts", "payment_mode",
        "amount_paid", "payment_status", "gateway_ref",
    ],
    "customers": [
        "customer_id", "full_name", "email", "phone", "city", "area",
        "pincode", "signup_date", "loyalty_tier", "is_active",
    ],
    "products": [
        "product_id", "product_name", "category", "sub_category", "brand",
        "mrp", "cost_price", "unit", "is_perishable", "launch_date",
    ],
    "stores": [
        "store_id", "store_name", "zone", "area", "pincode", "open_date",
        "manager_name", "capacity_orders_per_day",
    ],
}
INCREMENT_FILE = "orders_2026-10-01_increment.csv"
VALID_ORDER_STATUSES = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODES = {"UPI", "CARD", "CASH ON DELIVERY", "WALLET"}
VALID_PAYMENT_STATUSES = {"SUCCESS", "FAILED", "REFUNDED"}
_LOCK_GUARD = threading.RLock()
_LOCK_STATE = threading.local()
_P = ParamSpec("_P")
_R = TypeVar("_R")


@contextmanager
def _warehouse_lock():
    with _LOCK_GUARD:
        depth = getattr(_LOCK_STATE, "depth", 0)
        if depth == 0:
            lock_path = WAREHOUSE_DB.with_suffix(WAREHOUSE_DB.suffix + ".lock")
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            stream = lock_path.open("a+b")
            if os.name == "nt":
                lock_module = importlib.import_module("msvcrt")
                stream.seek(0, os.SEEK_END)
                if stream.tell() == 0:
                    stream.write(b"0")
                    stream.flush()
                stream.seek(0)
                lock_module.locking(stream.fileno(), lock_module.LK_LOCK, 1)
            else:
                lock_module = importlib.import_module("fcntl")
                lock_module.flock(stream.fileno(), lock_module.LOCK_EX)
            _LOCK_STATE.stream = stream
        _LOCK_STATE.depth = depth + 1
        try:
            yield
        finally:
            _LOCK_STATE.depth -= 1
            if _LOCK_STATE.depth == 0:
                stream = _LOCK_STATE.stream
                if os.name == "nt":
                    lock_module = importlib.import_module("msvcrt")
                    stream.seek(0)
                    lock_module.locking(stream.fileno(), lock_module.LK_UNLCK, 1)
                else:
                    lock_module = importlib.import_module("fcntl")
                    lock_module.flock(stream.fileno(), lock_module.LOCK_UN)
                stream.close()
                del _LOCK_STATE.stream


def _serialized(function: Callable[_P, _R]) -> Callable[_P, _R]:
    @wraps(function)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        with _warehouse_lock():
            return function(*args, **kwargs)

    return wrapper


def _normalise(value: Any) -> str:
    return str(value).strip().upper() if pd.notna(value) else ""


def _number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _local_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="Asia/Kolkata")


def _timestamp(series: pd.Series, *, errors: str = "coerce") -> pd.Series:
    parsed = pd.to_datetime(series, errors=errors)
    if parsed.dt.tz is None:
        return parsed.dt.tz_localize("Asia/Kolkata")
    return parsed.dt.tz_convert("Asia/Kolkata")


def _read_source(name: str, path: Path | None = None) -> pd.DataFrame:
    source_path = path or DATA_DIR / SOURCE_FILES[name]
    if not source_path.is_file():
        raise FileNotFoundError(f"Required NammaMart source file is missing: {source_path}")
    frame = pd.read_csv(source_path, dtype=str, keep_default_na=False)
    expected = EXPECTED_HEADERS[name]
    if list(frame.columns) != expected:
        raise ValueError(
            f"Unexpected schema in {source_path.name}: expected {expected}, "
            f"received {list(frame.columns)}"
        )
    frame["_source_file"] = source_path.name
    frame["_source_row"] = range(2, len(frame) + 2)
    return frame


def _record_check(
    checks: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    frame: pd.DataFrame,
    source_name: str,
    mask: pd.Series,
    *,
    check_name: str,
    dimension: str,
    severity: str,
    column: str,
    problem: str,
    proposed_rule: str,
) -> pd.Series:
    selected = mask.fillna(False).astype(bool)
    failed = int(selected.sum())
    checks.append({
        "check_name": check_name,
        "dimension": dimension,
        "severity": severity,
        "rows_checked": int(len(frame)),
        "rows_failed": failed,
    })
    if failed:
        key_column = {
            "orders": "order_id",
            "payments": "payment_id",
            "customers": "customer_id",
            "products": "product_id",
            "stores": "store_id",
        }[source_name]
        for row in frame.loc[selected].to_dict("records"):
            issues.append({
                "file": row.get("_source_file", SOURCE_FILES.get(source_name, "")),
                "row_key": row.get(key_column, ""),
                "source_row": row.get("_source_row", ""),
                "column": column,
                "problem": problem,
                "quality_dimension": dimension,
                "proposed_rule": proposed_rule,
                "severity": severity,
            })
    return selected


def _add_failure(
    critical_mask: pd.Series,
    reasons: pd.Series,
    selected: pd.Series,
    reason: str,
    severity: str,
) -> tuple[pd.Series, pd.Series]:
    if severity == "CRITICAL":
        critical_mask |= selected
        reasons.loc[selected] = reasons.loc[selected].map(
            lambda value: f"{value}; {reason}" if value else reason
        )
    return critical_mask, reasons


def _check(
    checks: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    frame: pd.DataFrame,
    source_name: str,
    mask: pd.Series,
    *,
    check_name: str,
    dimension: str,
    severity: str,
    column: str,
    problem: str,
    proposed_rule: str,
    critical_mask: pd.Series,
    reasons: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    selected = _record_check(
        checks, issues, frame, source_name, mask,
        check_name=check_name, dimension=dimension, severity=severity,
        column=column, problem=problem, proposed_rule=proposed_rule,
    )
    return _add_failure(
        critical_mask, reasons, selected, problem, severity
    )


def _previous_order_volume(dag_id: str, run_id: str) -> int | None:
    con = _connection()
    exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='main' AND table_name='dq_results'"
    ).fetchone()[0]
    if not exists:
        con.close()
        return None
    row = con.execute(
        """
        SELECT rows_checked
        FROM dq_results
        WHERE dag_id=? AND check_name='orders_volume_anomaly'
          AND status='PASS' AND run_id<>?
        ORDER BY checked_at DESC
        LIMIT 1
        """,
        [dag_id, run_id],
    ).fetchone()
    con.close()
    return int(row[0]) if row else None


def _previous_order_price_medians() -> dict[str, float]:
    con = _connection()
    exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='main' AND table_name='etl_fact_orders'"
    ).fetchone()[0]
    if not exists:
        con.close()
        return {}
    medians = {
        str(product_id): float(median)
        for product_id, median in con.execute(
            """
            SELECT product_id, MEDIAN(unit_price)
            FROM etl_fact_orders
            WHERE unit_price > 0
            GROUP BY product_id
            """
        ).fetchall()
        if median is not None
    }
    con.close()
    return medians


def _validate_frame(
    source_name: str,
    frame: pd.DataFrame,
    reference_ids: dict[str, set[str]],
    order_lookup: dict[str, dict[str, Any]],
    checks: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    check_prefix: str | None = None,
    prior_order_count: int | None = None,
    prior_price_medians: dict[str, float] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = frame.copy()
    critical = pd.Series(False, index=frame.index)
    reasons = pd.Series("", index=frame.index, dtype=object)

    def check(mask: pd.Series, *, name: str, dimension: str, severity: str,
              column: str, problem: str, rule: str) -> None:
        nonlocal critical, reasons
        critical, reasons = _check(
            checks, issues, frame, source_name, mask,
            check_name=f"{check_prefix or source_name}_{name}", dimension=dimension,
            severity=severity, column=column, problem=problem,
            proposed_rule=rule, critical_mask=critical, reasons=reasons,
        )

    def blank(column: str) -> pd.Series:
        return frame[column].astype(str).str.strip().eq("")

    if source_name == "orders":
        status = frame["order_status"].map(_normalise)
        mode = frame["payment_mode"].map(_normalise).replace({"COD": "CASH ON DELIVERY"})
        quantity = _number(frame["quantity"])
        unit_price = _number(frame["unit_price"])
        discount = _number(frame["discount_pct"])
        amount = _number(frame["order_amount"])
        minutes = _number(frame["delivery_minutes"])
        order_ts = _timestamp(frame["order_ts"])
        expected_amount = (quantity * unit_price * (1 - discount / 100)).round(2)

        for column in ("order_id", "customer_id", "store_id", "product_id"):
            check(blank(column), name=f"completeness_{column}",
                  dimension="completeness", severity="CRITICAL",
                  column=column, problem=f"Missing required {column}",
                  rule=f"{column} must not be blank.")
        check(frame["order_id"].duplicated(keep=False),
              name="uniqueness_order_id", dimension="uniqueness",
              severity="CRITICAL", column="order_id",
              problem="Duplicate order_id within this source file",
              rule="order_id must be unique within each source export.")
        check(~status.isin(VALID_ORDER_STATUSES), name="validity_order_status",
              dimension="validity", severity="CRITICAL", column="order_status",
              problem="order_status is not an allowed value",
              rule="Use DELIVERED, CANCELLED, or RETURNED.")
        check(~mode.isin(VALID_PAYMENT_MODES), name="validity_payment_mode",
              dimension="validity", severity="CRITICAL", column="payment_mode",
              problem="payment_mode is not an allowed value",
              rule="Use UPI, Card, Cash on Delivery, or Wallet.")
        check(order_ts.isna(), name="validity_order_ts", dimension="validity",
              severity="CRITICAL", column="order_ts",
              problem="order_ts is missing or cannot be parsed",
              rule="order_ts must parse as a timestamp.")
        check(quantity.isna() | (quantity <= 0), name="accuracy_quantity",
              dimension="accuracy", severity="CRITICAL", column="quantity",
              problem="quantity must be a positive number",
              rule="quantity must be greater than zero.")
        check(discount.isna() | ~discount.between(0, 100),
              name="accuracy_discount_pct", dimension="accuracy",
              severity="CRITICAL", column="discount_pct",
              problem="discount_pct is outside 0-100",
              rule="discount_pct must be between 0 and 100.")
        check(amount.isna() | expected_amount.isna()
              | ((amount - expected_amount).abs() > 0.01),
              name="accuracy_order_amount", dimension="accuracy",
              severity="CRITICAL", column="order_amount",
              problem="order_amount does not match discounted quantity * price",
              rule="order_amount must equal rounded quantity × unit_price × (1-discount_pct/100).")
        check(status.eq("DELIVERED") & minutes.isna(),
              name="completeness_delivery_minutes", dimension="completeness",
              severity="CRITICAL", column="delivery_minutes",
              problem="DELIVERED order has no delivery_minutes",
              rule="DELIVERED orders require delivery_minutes.")
        check(status.eq("DELIVERED") & minutes.ge(120),
              name="accuracy_delivery_minutes", dimension="accuracy",
              severity="CRITICAL", column="delivery_minutes",
              problem="DELIVERED order delivery time is at least 120 minutes",
              rule="delivery_minutes must be below 120.")
        for column, ref_name in (
            ("customer_id", "customers"),
            ("product_id", "products"),
            ("store_id", "stores"),
        ):
            check(~frame[column].astype(str).str.strip().isin(reference_ids[ref_name]),
                  name=f"consistency_{column}", dimension="consistency",
                  severity="CRITICAL", column=column,
                  problem=f"{column} does not exist in the accepted {ref_name} dimension",
                  rule=f"{column} must reference a valid unique {ref_name} key.")
        check(order_ts > _local_now(),
              name="timeliness_future_order", dimension="timeliness",
              severity="CRITICAL", column="order_ts",
              problem="order_ts is in the future",
              rule="order_ts must not be later than the current time.")
        latest = order_ts.max()
        stale = pd.Series(False, index=frame.index)
        if pd.notna(latest) and (_local_now() - latest).days > 1:
            stale[:] = True
        check(stale, name="timeliness_stale_orders", dimension="timeliness",
              severity="WARNING", column="order_ts",
              problem="Latest order is more than one day old",
              rule="Warn when source freshness exceeds one day.")
        if check_prefix is None:
            volume_anomaly = (
                prior_order_count is not None
                and len(frame) < 0.1 * prior_order_count
            )
            check(pd.Series(volume_anomaly, index=frame.index),
                  name="volume_anomaly", dimension="volume",
                  severity="WARNING", column="order_id",
                  problem="Order volume is below 10% of the previous snapshot",
                  rule="Warn when the current orders file has fewer than 10% "
                       "of the rows in the previous ETL snapshot.")
            prior_price_medians = prior_price_medians or {}
            expected_price = frame["product_id"].astype(str).map(
                prior_price_medians
            )
            price_drift = _number(frame["unit_price"]) > 5 * expected_price
            check(price_drift, name="distribution_unit_price",
                  dimension="distribution", severity="WARNING",
                  column="unit_price",
                  problem="unit_price exceeds 5x its prior product median",
                  rule="Warn when unit_price exceeds five times the prior "
                       "accepted median for that product.")

    elif source_name == "customers":
        signup = _date(frame["signup_date"])
        pincode = frame["pincode"].astype(str).str.strip()
        email = frame["email"].astype(str).str.strip()
        phone = frame["phone"].astype(str).str.strip()
        tier = frame["loyalty_tier"].astype(str).str.strip()
        check(blank("customer_id") | blank("full_name"),
              name="completeness_customer", dimension="completeness",
              severity="CRITICAL", column="customer_id",
              problem="customer_id or full_name is missing",
              rule="Customer identifier and name are required.")
        check(frame["customer_id"].duplicated(keep=False),
              name="uniqueness_customer_id", dimension="uniqueness",
              severity="CRITICAL", column="customer_id",
              problem="Duplicate customer_id",
              rule="customer_id must be unique.")
        check(~email.str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
              name="validity_email", dimension="validity",
              severity="WARNING", column="email",
              problem="Email is missing or malformed",
              rule="Warn unless email matches a basic address pattern.")
        check(~phone.str.fullmatch(r"\d{10}"),
              name="validity_phone", dimension="validity",
              severity="WARNING", column="phone",
              problem="Phone is not a 10-digit number",
              rule="Warn unless phone contains exactly 10 digits.")
        check(~pincode.str.fullmatch(r"\d{6}"),
              name="validity_pincode", dimension="validity",
              severity="WARNING", column="pincode",
              problem="Pincode is not six digits",
              rule="Warn unless pincode contains exactly six digits.")
        check(tier.ne(tier.str.upper()),
              name="consistency_loyalty_tier_case", dimension="consistency",
              severity="WARNING", column="loyalty_tier",
              problem="loyalty_tier casing is inconsistent",
              rule="Standardise loyalty_tier to uppercase.")
        check(signup.isna(), name="validity_signup_date", dimension="validity",
              severity="CRITICAL", column="signup_date",
              problem="signup_date is missing or invalid",
              rule="signup_date must parse as a date.")
        check(signup.dt.date > _local_now().date(),
              name="timeliness_future_signup", dimension="timeliness",
              severity="CRITICAL", column="signup_date",
              problem="signup_date is in the future",
              rule="signup_date must not be later than the current date.")

    elif source_name == "products":
        mrp = _number(frame["mrp"])
        cost = _number(frame["cost_price"])
        check(blank("product_id") | blank("category"),
              name="completeness_product", dimension="completeness",
              severity="CRITICAL", column="category",
              problem="product_id or category is missing",
              rule="Product key and category are required.")
        check(frame["product_id"].duplicated(keep=False),
              name="uniqueness_product_id", dimension="uniqueness",
              severity="CRITICAL", column="product_id",
              problem="Duplicate product_id",
              rule="product_id must be unique.")
        check(mrp.isna() | mrp.le(0), name="accuracy_mrp",
              dimension="accuracy", severity="CRITICAL", column="mrp",
              problem="mrp is missing or not positive",
              rule="mrp must be greater than zero.")
        check(cost.isna() | cost.le(0), name="accuracy_cost_price",
              dimension="accuracy", severity="CRITICAL", column="cost_price",
              problem="cost_price is missing or not positive",
              rule="cost_price must be greater than zero.")
        check(cost > mrp, name="accuracy_cost_below_mrp",
              dimension="accuracy", severity="WARNING", column="cost_price",
              problem="cost_price exceeds mrp",
              rule="Warn when cost_price exceeds mrp; business owner must confirm promotions/markdowns.")
        launch = _date(frame["launch_date"])
        check(launch.isna(), name="validity_launch_date",
              dimension="validity", severity="CRITICAL", column="launch_date",
              problem="launch_date is missing or invalid",
              rule="launch_date must parse as a date.")
        check(launch.dt.date > _local_now().date(),
              name="timeliness_future_launch", dimension="timeliness",
              severity="WARNING", column="launch_date",
              problem="launch_date is in the future",
              rule="Flag future product launch dates for review.")

    elif source_name == "stores":
        pincode = frame["pincode"].astype(str).str.strip()
        capacity = _number(frame["capacity_orders_per_day"])
        check(blank("store_id") | blank("store_name") | blank("zone"),
              name="completeness_store", dimension="completeness",
              severity="CRITICAL", column="zone",
              problem="store_id, store_name, or zone is missing",
              rule="Store key, name, and zone are required.")
        check(frame["store_id"].duplicated(keep=False),
              name="uniqueness_store_id", dimension="uniqueness",
              severity="CRITICAL", column="store_id",
              problem="Duplicate store_id",
              rule="store_id must be unique.")
        check(~pincode.str.fullmatch(r"\d{6}"),
              name="validity_store_pincode", dimension="validity",
              severity="WARNING", column="pincode",
              problem="Store pincode is not six digits",
              rule="Warn unless store pincode contains six digits.")
        check(capacity.isna() | capacity.le(0),
              name="accuracy_store_capacity", dimension="accuracy",
              severity="CRITICAL", column="capacity_orders_per_day",
              problem="Store capacity is missing or not positive",
              rule="capacity_orders_per_day must be greater than zero.")
        open_date = _date(frame["open_date"])
        check(open_date.isna(), name="validity_open_date",
              dimension="validity", severity="CRITICAL", column="open_date",
              problem="open_date is missing or invalid",
              rule="open_date must parse as a date.")
        check(open_date.dt.date > _local_now().date(),
              name="timeliness_future_open_date", dimension="timeliness",
              severity="WARNING", column="open_date",
              problem="open_date is in the future",
              rule="Flag future store opening dates for review.")

    elif source_name == "payments":
        amount = _number(frame["amount_paid"])
        payment_ts = _timestamp(frame["payment_ts"])
        status = frame["payment_status"].map(_normalise)
        mode = frame["payment_mode"].map(_normalise).replace({"COD": "CASH ON DELIVERY"})
        check(blank("payment_id") | blank("order_id"),
              name="completeness_payment", dimension="completeness",
              severity="CRITICAL", column="payment_id",
              problem="payment_id or order_id is missing",
              rule="Every payment requires payment_id and order_id.")
        check(frame["payment_id"].duplicated(keep=False),
              name="uniqueness_payment_id", dimension="uniqueness",
              severity="CRITICAL", column="payment_id",
              problem="Duplicate payment_id",
              rule="payment_id must be unique.")
        check(~status.isin(VALID_PAYMENT_STATUSES),
              name="validity_payment_status", dimension="validity",
              severity="CRITICAL", column="payment_status",
              problem="payment_status is not an allowed value",
              rule="Use SUCCESS, FAILED, or REFUNDED.")
        check(~mode.isin(VALID_PAYMENT_MODES),
              name="validity_payment_mode", dimension="validity",
              severity="CRITICAL", column="payment_mode",
              problem="payment_mode is not an allowed value",
              rule="Use UPI, Card, Cash on Delivery, or Wallet.")
        check(amount.isna() | amount.lt(0), name="accuracy_payment_amount",
              dimension="accuracy", severity="CRITICAL", column="amount_paid",
              problem="amount_paid is missing or negative",
              rule="amount_paid must be a non-negative number.")
        check(payment_ts.isna(), name="validity_payment_ts",
              dimension="validity", severity="CRITICAL", column="payment_ts",
              problem="payment_ts is missing or invalid",
              rule="payment_ts must parse as a timestamp.")
        check(~frame["order_id"].astype(str).isin(order_lookup),
              name="consistency_payment_order", dimension="consistency",
              severity="CRITICAL", column="order_id",
              problem="Payment references an unknown order_id",
              rule="Payment order_id must exist in the orders export.")
        expected = frame["order_id"].map(
            lambda order_id: order_lookup.get(str(order_id), {}).get("order_amount")
        )
        expected_amount = pd.to_numeric(expected, errors="coerce")
        check(status.eq("SUCCESS") & expected_amount.notna()
              & ((amount - expected_amount).abs() > 0.01),
              name="consistency_payment_amount", dimension="consistency",
              severity="CRITICAL", column="amount_paid",
              problem="SUCCESS payment amount differs from its order amount",
              rule="A SUCCESS payment must equal the current order_amount.")
        check(payment_ts > _local_now(),
              name="timeliness_future_payment", dimension="timeliness",
              severity="CRITICAL", column="payment_ts",
              problem="payment_ts is in the future",
              rule="payment_ts must not be later than the current time.")

    frame["failure_reason"] = reasons
    frame["dq_status"] = "PASS"
    frame.loc[critical, "dq_status"] = "FAIL"
    frame["warning_reason"] = ""
    warning_names = {
        item["check_name"]: item for item in checks
        if item["severity"] == "WARNING" and item["rows_failed"]
    }
    if warning_names:
        warning_rows = pd.DataFrame(issues)
        warning_rows = warning_rows[
            (warning_rows["file"].isin(frame["_source_file"].unique()))
            & (warning_rows["severity"] == "WARNING")
        ]
        key_column = {
            "orders": "order_id", "payments": "payment_id",
            "customers": "customer_id", "products": "product_id",
            "stores": "store_id",
        }[source_name]
        warning_by_key: dict[str, list[str]] = {}
        for issue in warning_rows.to_dict("records"):
            warning_by_key.setdefault(str(issue["row_key"]), []).append(issue["problem"])
        frame["warning_reason"] = frame[key_column].map(
            lambda value: "; ".join(dict.fromkeys(warning_by_key.get(str(value), [])))
        )
    return frame.loc[~critical].copy(), frame.loc[critical].copy()


def _create_audit_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR NOT NULL,
            dag_id VARCHAR NOT NULL,
            check_name VARCHAR NOT NULL,
            dimension VARCHAR NOT NULL,
            severity VARCHAR NOT NULL,
            rows_checked BIGINT NOT NULL,
            rows_failed BIGINT NOT NULL,
            status VARCHAR NOT NULL,
            checked_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (run_id, dag_id, check_name)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR NOT NULL,
            dag_id VARCHAR NOT NULL,
            start_time TIMESTAMPTZ,
            end_time TIMESTAMPTZ NOT NULL,
            rows_extracted BIGINT NOT NULL,
            rows_loaded BIGINT NOT NULL,
            rows_quarantined BIGINT NOT NULL,
            status VARCHAR NOT NULL,
            PRIMARY KEY (run_id, dag_id)
        )
    """)


@_serialized
def _store_dq_results(
    checks: list[dict[str, Any]], run_id: str, dag_id: str
) -> None:
    checked_at = datetime.now(timezone.utc)
    con = duckdb.connect(str(WAREHOUSE_DB))
    _create_audit_tables(con)
    for check in checks:
        status = "FAIL" if check["severity"] == "CRITICAL" and check["rows_failed"] else (
            "WARN" if check["rows_failed"] else "PASS"
        )
        con.execute(
            """
            INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (run_id, dag_id, check_name) DO UPDATE SET
                dimension=excluded.dimension, severity=excluded.severity,
                rows_checked=excluded.rows_checked, rows_failed=excluded.rows_failed,
                status=excluded.status, checked_at=excluded.checked_at
            """,
            [
                run_id, dag_id, check["check_name"], check["dimension"],
                check["severity"], check["rows_checked"], check["rows_failed"],
                status, checked_at,
            ],
        )
    con.close()


@_serialized
def _store_issue_details(
    issues: list[dict[str, Any]], run_id: str, dag_id: str
) -> None:
    con = duckdb.connect(str(WAREHOUSE_DB))
    con.execute("""
        CREATE TABLE IF NOT EXISTS dq_issue_details (
            run_id VARCHAR NOT NULL,
            dag_id VARCHAR NOT NULL,
            source_file VARCHAR NOT NULL,
            row_key VARCHAR,
            source_row BIGINT NOT NULL,
            column_name VARCHAR NOT NULL,
            problem VARCHAR NOT NULL,
            quality_dimension VARCHAR NOT NULL,
            proposed_rule VARCHAR NOT NULL,
            severity VARCHAR NOT NULL,
            PRIMARY KEY (run_id, dag_id, source_file, source_row, column_name, problem)
        )
    """)
    if issues:
        issue_frame = pd.DataFrame(issues).rename(columns={
            "file": "source_file",
            "column": "column_name",
        })
        issue_frame.insert(0, "dag_id", dag_id)
        issue_frame.insert(0, "run_id", run_id)
        issue_frame = issue_frame[[
            "run_id", "dag_id", "source_file", "row_key", "source_row",
            "column_name", "problem", "quality_dimension", "proposed_rule",
            "severity",
        ]]
        con.register("issue_frame", issue_frame)
        con.execute("""
            INSERT INTO dq_issue_details
            SELECT * FROM issue_frame
            ON CONFLICT (run_id, dag_id, source_file, source_row, column_name, problem)
            DO UPDATE SET quality_dimension=excluded.quality_dimension,
                          proposed_rule=excluded.proposed_rule,
                          severity=excluded.severity
        """)
        con.unregister("issue_frame")
    con.close()


def _mask_email(value: Any) -> str:
    email = str(value).strip()
    if "@" not in email:
        return "***"
    local, domain = email.split("@", 1)
    return f"{local[:2]}***@{domain}" if local else f"***@{domain}"


def _mask_phone(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value))
    if len(digits) == 10:
        return f"{digits[:2]}******{digits[-2:]}"
    return "***"


def extract_source(name: str, *, increment: bool = False) -> dict[str, Any]:
    filename = INCREMENT_FILE if increment else SOURCE_FILES[name]
    path = DATA_DIR / filename
    frame = _read_source(name, path)
    print(f"[Extract] {filename}: {len(frame)} rows")
    return {"file": filename, "rows": len(frame)}


@_serialized
def validate_etl_sources(run_id: str, dag_id: str) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    raw: dict[str, pd.DataFrame] = {}
    prior_order_count = _previous_order_volume(dag_id, run_id)
    prior_price_medians = _previous_order_price_medians()
    for name in ("customers", "products", "stores", "orders", "payments"):
        raw[name] = _read_source(name)
    increment_path = DATA_DIR / INCREMENT_FILE
    if increment_path.exists():
        raw["increment"] = _read_source("orders", increment_path)

    accepted_ids: dict[str, set[str]] = {}
    for name, key in (
        ("customers", "customer_id"), ("products", "product_id"),
        ("stores", "store_id"),
    ):
        frame = raw[name]
        duplicate = frame[key].duplicated(keep=False)
        accepted_ids[name] = set(
            frame.loc[~duplicate, key].astype(str).str.strip()
        )

    orders_for_lookup = pd.concat(
        [raw["orders"]] + ([raw["increment"]] if "increment" in raw else []),
        ignore_index=True,
    )
    current_order_lookup: dict[str, dict[str, Any]] = {}
    for row in orders_for_lookup.to_dict("records"):
        current_order_lookup[str(row["order_id"])] = {
            "order_amount": pd.to_numeric(row["order_amount"], errors="coerce")
        }

    reference_ids = {
        "customers": accepted_ids["customers"],
        "products": accepted_ids["products"],
        "stores": accepted_ids["stores"],
    }
    accepted: dict[str, pd.DataFrame] = {}
    rejected: dict[str, pd.DataFrame] = {}
    for name in ("customers", "products", "stores"):
        accepted[name], rejected[name] = _validate_frame(
            name, raw[name], reference_ids, current_order_lookup, checks, issues
        )

    reference_ids = {
        "customers": set(accepted["customers"]["customer_id"].astype(str)),
        "products": set(accepted["products"]["product_id"].astype(str)),
        "stores": set(accepted["stores"]["store_id"].astype(str)),
    }
    accepted["orders"], rejected["orders"] = _validate_frame(
        "orders", raw["orders"], reference_ids, current_order_lookup, checks,
        issues, prior_order_count=prior_order_count,
        prior_price_medians=prior_price_medians,
    )
    if "increment" in raw:
        accepted["increment"], rejected["increment"] = _validate_frame(
            "orders", raw["increment"], reference_ids, current_order_lookup,
            checks, issues, check_prefix="increment",
        )

    accepted["payments"], rejected["payments"] = _validate_frame(
        "payments", raw["payments"], reference_ids, current_order_lookup, checks, issues
    )

    for name, frame in accepted.items():
        source_name = "orders" if name == "increment" else name
        filename = INCREMENT_FILE if name == "increment" else SOURCE_FILES[source_name]
        frame = frame.copy()
        frame["_source_file"] = filename
        frame["source_row"] = frame["_source_row"]
        frame["email_masked"] = frame["email"].map(_mask_email) if name == "customers" else ""
        frame["phone_masked"] = frame["phone"].map(_mask_phone) if name == "customers" else ""
        if name == "customers":
            frame = frame[[
                "customer_id", "email_masked", "phone_masked", "loyalty_tier",
                "is_active", "dq_status", "warning_reason", "_source_file",
                "source_row",
            ]]
        clean_path = PROFILE_DIR / "working" / run_id / f"{name}_clean.csv"
        clean_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(clean_path, index=False)
        accepted[name] = frame
    for name, frame in rejected.items():
        if frame.empty:
            continue
        source_name = "orders" if name == "increment" else name
        filename = INCREMENT_FILE if name == "increment" else SOURCE_FILES[source_name]
        frame = frame.copy()
        frame["failure_reason"] = frame["failure_reason"].replace("", "Critical data-quality check failed")
        quarantine_path = QUARANTINE_DIR / f"{filename.removesuffix('.csv')}_failed_{run_id}.csv"
        quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(quarantine_path, index=False)

    issue_frame = pd.DataFrame(issues, columns=[
        "file", "row_key", "source_row", "column", "problem", "quality_dimension",
        "proposed_rule", "severity",
    ])
    customer_rows = issue_frame["file"].eq(SOURCE_FILES["customers"])
    issue_frame.loc[customer_rows, "row_key"] = issue_frame.loc[
        customer_rows, "source_row"
    ].map(lambda source_row: f"customers.csv:row_{source_row}")
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    issue_frame.to_csv(PROFILE_DIR / "nammamart_data_issues.csv", index=False)
    _store_issue_details(issues, run_id, dag_id)
    _store_dq_results(checks, run_id, dag_id)
    quarantined = sum(len(frame) for frame in rejected.values())
    extracted = sum(len(frame) for frame in raw.values())
    result = {
        "run_id": run_id,
        "dag_id": dag_id,
        "rows_extracted": extracted,
        "rows_quarantined": quarantined,
        "accepted_paths": {
            name: str(PROFILE_DIR / "working" / run_id / f"{name}_clean.csv")
            for name in accepted
        },
        "accepted_counts": {name: len(frame) for name, frame in accepted.items()},
    }
    print(
        f"[Validate] extracted={extracted}, quarantined={quarantined}, "
        f"accepted={sum(result['accepted_counts'].values())}"
    )
    return result


def _read_clean(path: str) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    return frame


def _standardise(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for column in ("order_status", "payment_status", "payment_mode", "loyalty_tier", "zone"):
        if column in result:
            result[column] = result[column].astype(str).str.strip().str.upper()
    if "payment_mode" in result:
        result["payment_mode"] = result["payment_mode"].replace({
            "COD": "CASH ON DELIVERY",
        })
    return result


def _connection() -> duckdb.DuckDBPyConnection:
    WAREHOUSE_DB.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(WAREHOUSE_DB))


@_serialized
def transform_load_etl(validation: dict[str, Any], run_id: str) -> dict[str, int]:
    sources = {
        name: _standardise(_read_clean(path))
        for name, path in validation["accepted_paths"].items()
    }
    customers = sources["customers"].drop_duplicates("customer_id")
    products = sources["products"].drop_duplicates("product_id")
    stores = sources["stores"].drop_duplicates("store_id")
    order_frames = [sources["orders"]]
    if "increment" in sources:
        order_frames.append(sources["increment"])
    orders = pd.concat(order_frames, ignore_index=True)
    orders["_increment_priority"] = orders["_source_file"].eq(INCREMENT_FILE).astype(int)
    orders = orders.sort_values(["_increment_priority", "_source_row"]).drop_duplicates(
        "order_id", keep="last"
    )
    orders["quantity"] = pd.to_numeric(orders["quantity"], errors="raise").astype(int)
    orders["unit_price"] = pd.to_numeric(orders["unit_price"], errors="raise")
    orders["discount_pct"] = pd.to_numeric(orders["discount_pct"], errors="raise")
    orders["order_amount"] = pd.to_numeric(orders["order_amount"], errors="raise").round(2)
    orders["delivery_minutes"] = pd.to_numeric(orders["delivery_minutes"], errors="coerce")
    orders["order_ts"] = _timestamp(orders["order_ts"], errors="raise")
    orders["order_date"] = orders["order_ts"].dt.date.astype(str)

    customer_dim = customers[[
        "customer_id", "email_masked", "phone_masked", "loyalty_tier",
        "is_active",
    ]].copy()
    product_dim = products[[
        "product_id", "product_name", "category", "sub_category", "brand",
        "cost_price", "mrp", "unit", "is_perishable",
    ]].copy()
    product_dim["cost_price"] = pd.to_numeric(product_dim["cost_price"], errors="raise")
    store_dim = stores[[
        "store_id", "store_name", "zone", "area", "pincode",
        "capacity_orders_per_day",
    ]].copy()
    store_dim["capacity_orders_per_day"] = pd.to_numeric(
        store_dim["capacity_orders_per_day"], errors="raise"
    ).astype(int)

    merged = orders.merge(customer_dim, on="customer_id", how="inner")
    merged = merged.merge(product_dim, on="product_id", how="inner")
    merged = merged.merge(store_dim, on="store_id", how="inner")
    merged["net_revenue"] = merged["order_amount"].round(2)
    merged["gross_margin"] = (
        merged["net_revenue"] - merged["quantity"] * merged["cost_price"]
    ).round(2)
    merged["within_15_min"] = merged["delivery_minutes"].le(15)
    merged["source_row"] = pd.to_numeric(merged["source_row"], errors="raise").astype(int)
    merged["ingested_at"] = datetime.now(timezone.utc)
    fact = merged[[
        "order_id", "customer_id", "product_id", "store_id", "order_date",
        "order_ts", "order_status", "quantity", "unit_price", "discount_pct",
        "net_revenue", "cost_price", "gross_margin", "payment_mode",
        "delivery_minutes", "within_15_min", "_source_file", "source_row",
        "ingested_at",
    ]].copy()
    fact["_run_id"] = run_id

    con = _connection()
    _create_audit_tables(con)
    con.register("customer_dim_frame", customer_dim)
    con.register("product_dim_frame", product_dim)
    con.register("store_dim_frame", store_dim)
    con.register("fact_frame", fact)
    payments = sources["payments"][[
        "payment_id", "order_id", "payment_ts", "payment_mode",
        "amount_paid", "payment_status", "gateway_ref", "dq_status",
    ]].copy()
    payments["payment_mode"] = payments["payment_mode"].map(_normalise).replace({
        "COD": "CASH ON DELIVERY",
    })
    payments["amount_paid"] = pd.to_numeric(payments["amount_paid"], errors="raise")
    payments["payment_ts"] = _timestamp(payments["payment_ts"], errors="raise")
    con.register("payment_frame", payments)
    con.execute("BEGIN TRANSACTION")
    con.execute("CREATE OR REPLACE TABLE etl_stg_payments AS SELECT * FROM payment_frame")
    for table, frame_name in (
        ("etl_dim_customer", "customer_dim_frame"),
        ("etl_dim_product", "product_dim_frame"),
        ("etl_dim_store", "store_dim_frame"),
        ("etl_fact_orders", "fact_frame"),
    ):
        con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {frame_name}")
    con.execute("COMMIT")
    con.unregister("customer_dim_frame")
    con.unregister("product_dim_frame")
    con.unregister("store_dim_frame")
    con.unregister("fact_frame")
    con.unregister("payment_frame")
    con.close()
    base_ids = set(sources["orders"]["order_id"].astype(str))
    final_increment_ids = set(
        fact.loc[fact["_source_file"].eq(INCREMENT_FILE), "order_id"].astype(str)
    )
    counts = {
        "rows_loaded": len(fact),
        "orders_inserted": len(final_increment_ids - base_ids),
        "orders_updated": len(final_increment_ids & base_ids),
    }
    print(f"[ETL Load] fact rows={counts['rows_loaded']}")
    return counts


@_serialized
def build_etl_reports(run_id: str, dag_id: str) -> dict[str, int]:
    con = _connection()
    con.execute("""
        CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
        SELECT f.order_date, s.zone, f.store_id, s.store_name,
               COUNT(*) AS delivered_orders,
               ROUND(SUM(f.net_revenue), 2) AS net_revenue
        FROM etl_fact_orders f
        JOIN etl_dim_store s USING (store_id)
        WHERE f.order_status = 'DELIVERED'
        GROUP BY f.order_date, s.zone, f.store_id, s.store_name
    """)
    con.execute("""
        CREATE OR REPLACE TABLE etl_category_margin AS
        SELECT p.category, COUNT(*) AS delivered_orders,
               SUM(f.quantity) AS units_sold,
               ROUND(SUM(f.net_revenue), 2) AS net_revenue,
               ROUND(SUM(f.gross_margin), 2) AS gross_margin
        FROM etl_fact_orders f
        JOIN etl_dim_product p USING (product_id)
        WHERE f.order_status = 'DELIVERED'
        GROUP BY p.category
    """)
    con.execute("""
        CREATE OR REPLACE TABLE etl_delivery_sla AS
        SELECT f.store_id, s.store_name,
               ROUND(AVG(f.delivery_minutes), 2) AS avg_delivery_minutes,
               COUNT(*) FILTER (WHERE f.delivery_minutes <= 15) AS within_15_orders,
               COUNT(*) AS delivered_orders,
               ROUND(100.0 * COUNT(*) FILTER (WHERE f.delivery_minutes <= 15)
                     / NULLIF(COUNT(*), 0), 2) AS within_15_pct
        FROM etl_fact_orders f
        JOIN etl_dim_store s USING (store_id)
        WHERE f.order_status = 'DELIVERED'
        GROUP BY f.store_id, s.store_name
    """)
    con.execute("""
        CREATE OR REPLACE TABLE etl_top_customers AS
        SELECT f.customer_id, c.loyalty_tier,
               c.email_masked, ROUND(SUM(f.net_revenue), 2) AS total_spend
        FROM etl_fact_orders f
        JOIN etl_dim_customer c USING (customer_id)
        WHERE f.order_status = 'DELIVERED'
        GROUP BY f.customer_id, c.loyalty_tier, c.email_masked
        ORDER BY total_spend DESC
        LIMIT 10
    """)
    con.execute("""
        CREATE OR REPLACE TABLE etl_payment_mode_revenue AS
        SELECT payment_mode, COUNT(*) AS delivered_orders,
               ROUND(SUM(net_revenue), 2) AS net_revenue
        FROM etl_fact_orders
        WHERE order_status = 'DELIVERED'
        GROUP BY payment_mode
    """)
    con.execute("""
        CREATE OR REPLACE TABLE etl_rejected_rows_by_file_reason AS
        SELECT source_file, problem AS failure_reason,
               COUNT(DISTINCT source_row) AS rejected_rows
        FROM dq_issue_details
        WHERE run_id = ? AND dag_id = ? AND severity = 'CRITICAL'
        GROUP BY source_file, problem
    """, [run_id, dag_id])
    _build_reconciliation(
        con, "etl_fact_orders", "etl_payment_reconciliation", "etl_stg_payments"
    )
    counts = {
        "daily_zone_rows": con.execute("SELECT COUNT(*) FROM etl_daily_zone_revenue").fetchone()[0],
        "category_rows": con.execute("SELECT COUNT(*) FROM etl_category_margin").fetchone()[0],
        "reconciliation_rows": con.execute("SELECT COUNT(*) FROM etl_payment_reconciliation").fetchone()[0],
        "payment_mode_rows": con.execute("SELECT COUNT(*) FROM etl_payment_mode_revenue").fetchone()[0],
        "rejected_reason_rows": con.execute("SELECT COUNT(*) FROM etl_rejected_rows_by_file_reason").fetchone()[0],
    }
    for table in ("etl_daily_zone_revenue", "etl_category_margin"):
        print(f"[ETL Report] {table}:\n{con.execute(f'SELECT * FROM {table}').fetchdf().to_string(index=False)}")
    con.close()
    return counts


def _build_reconciliation(
    con: duckdb.DuckDBPyConnection,
    fact_table: str,
    output_table: str,
    payment_table: str,
) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE {output_table} AS
        WITH paid AS (
            SELECT order_id, COUNT(*) AS success_count,
                   SUM(TRY_CAST(amount_paid AS DOUBLE)) AS amount_paid,
                   STRING_AGG(DISTINCT UPPER(TRIM(payment_mode)), ', ') AS payment_mode
            FROM {payment_table}
            WHERE payment_status = 'SUCCESS' AND dq_status <> 'FAIL'
            GROUP BY order_id
        ),
        orders AS (
            SELECT f.order_id, f.order_date, f.order_status, f.net_revenue,
                   f.payment_mode AS order_payment_mode, paid.success_count,
                   paid.amount_paid, paid.payment_mode AS gateway_payment_mode
            FROM {fact_table} f
            LEFT JOIN paid USING (order_id)
            WHERE f.order_status = 'DELIVERED'
        )
        SELECT order_id, order_date,
               COALESCE(order_payment_mode, '') AS order_payment_mode,
               COALESCE(gateway_payment_mode, '') AS gateway_payment_mode,
               net_revenue AS order_amount, amount_paid,
               CASE
                 WHEN success_count IS NULL THEN 'missing_successful_payment'
                 WHEN success_count > 1 THEN 'multiple_successful_payments'
                 WHEN ABS(COALESCE(amount_paid, 0) - net_revenue) > 0.01 THEN 'amount_mismatch'
                 WHEN UPPER(order_payment_mode) <> UPPER(gateway_payment_mode) THEN 'payment_mode_mismatch'
               END AS mismatch_reason
        FROM orders
        WHERE success_count IS NULL OR success_count <> 1
           OR ABS(COALESCE(amount_paid, 0) - net_revenue) > 0.01
           OR UPPER(order_payment_mode) <> UPPER(gateway_payment_mode)
        UNION ALL
        SELECT p.order_id, NULL, '', UPPER(TRIM(p.payment_mode)),
               NULL, TRY_CAST(p.amount_paid AS DOUBLE), 'payment_without_delivered_order'
        FROM {payment_table} p
        LEFT JOIN {fact_table} f USING (order_id)
        WHERE p.payment_status = 'SUCCESS' AND p.dq_status <> 'FAIL'
          AND (f.order_id IS NULL OR f.order_status <> 'DELIVERED')
    """)


@_serialized
def load_raw_tables(run_id: str) -> dict[str, int]:
    sources = dict(SOURCE_FILES)
    increment = DATA_DIR / INCREMENT_FILE
    if increment.exists():
        sources["increment"] = INCREMENT_FILE
    con = _connection()
    _create_audit_tables(con)
    counts: dict[str, int] = {}
    con.execute("BEGIN TRANSACTION")
    try:
        for name, filename in sources.items():
            table = "raw_orders" if name == "increment" else f"raw_{name}"
            source_name = "orders" if name == "increment" else name
            path = DATA_DIR / filename
            with path.open("r", encoding="utf-8-sig", newline="") as stream:
                reader = csv.DictReader(stream)
                if reader.fieldnames != EXPECTED_HEADERS[source_name]:
                    raise ValueError(
                        f"Unexpected schema in {filename}: expected "
                        f"{EXPECTED_HEADERS[source_name]}, received {reader.fieldnames}"
                    )
                rows = [
                    tuple(row.get(column, "") or "" for column in reader.fieldnames)
                    for row in reader
                ]
            columns = EXPECTED_HEADERS[source_name]
            quoted = ", ".join(f'"{column}" VARCHAR' for column in columns)
            con.execute(
                f"CREATE TABLE IF NOT EXISTS {table} ({quoted}, "
                "_loaded_at TIMESTAMPTZ, _source_file VARCHAR, _run_id VARCHAR, "
                "_source_row BIGINT)"
            )
            placeholders = ", ".join("?" for _ in columns)
            column_list = ", ".join(f'"{column}"' for column in columns)
            con.executemany(
                f"INSERT INTO {table} ({column_list}, _loaded_at, _source_file, _run_id, _source_row) "
                f"VALUES ({placeholders}, CURRENT_TIMESTAMP, ?, ?, ?)",
                [
                    row + (filename, run_id, row_number)
                    for row_number, row in enumerate(rows, start=2)
                ],
            )
            counts[filename] = len(rows)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    print(f"[ELT Raw] appended rows: {counts}")
    return counts


@_serialized
def run_sql_layer(sql_name: str, run_id: str, dag_id: str) -> None:
    sql_path = SQL_DIR / sql_name
    if not sql_path.is_file():
        raise FileNotFoundError(f"Required SQL transformation is missing: {sql_path}")
    con = _connection()
    _create_audit_tables(con)
    con.execute("CREATE OR REPLACE TEMP TABLE pipeline_context AS SELECT ? AS run_id, ? AS dag_id, CURRENT_TIMESTAMP AS checked_at", [run_id, dag_id])
    sql = sql_path.read_text(encoding="utf-8")
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(sql)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


@_serialized
def build_elt_reports(run_id: str, dag_id: str) -> dict[str, int]:
    run_sql_layer("nammamart_marts.sql", run_id, dag_id)
    con = _connection()
    results = {
        table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in (
            "mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
            "mart_payment_reconciliation", "mart_top_customers",
            "mart_payment_mode_revenue", "mart_rejected_rows_by_file_reason",
        )
    }
    print("[ELT Report] mart_daily_store_kpis:\n" + con.execute(
        "SELECT * FROM mart_daily_store_kpis ORDER BY order_date, zone, store_id"
    ).fetchdf().to_string(index=False))
    print("[ELT Report] mart_category_margin:\n" + con.execute(
        "SELECT * FROM mart_category_margin ORDER BY category"
    ).fetchdf().to_string(index=False))
    con.close()
    return results


@_serialized
def get_staging_rejected_count(run_id: str) -> int:
    con = _connection()
    tables = (
        "stg_orders",
        "stg_payments",
        "stg_customers",
        "stg_products",
        "stg_stores",
    )
    try:
        existing = {
            row[0] for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='main' AND table_name IN (?, ?, ?, ?, ?)",
                list(tables),
            ).fetchall()
        }
        if not existing:
            return 0
        missing = sorted(set(tables) - existing)
        if missing:
            raise RuntimeError(
                "Cannot record ELT rejected-row count; staging tables are missing: "
                + ", ".join(missing)
            )
        return sum(
            con.execute(
                f"SELECT COUNT(*) FROM {table} "
                "WHERE dq_status='FAIL' AND _run_id=?",
                [run_id],
            ).fetchone()[0]
            for table in tables
        )
    finally:
        con.close()


def get_pipeline_run_counts(task_ids: list[str], ti: Any) -> int:
    results = ti.xcom_pull(task_ids=task_ids) or []
    return sum(
        item.get("rows", 0)
        for item in results
        if isinstance(item, dict)
    )


@_serialized
def write_run_audit(
    *,
    run_id: str,
    dag_id: str,
    start_time: Any,
    rows_extracted: int,
    rows_loaded: int,
    rows_quarantined: int,
    status: str,
) -> None:
    con = _connection()
    _create_audit_tables(con)
    con.execute(
        """
        INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (run_id, dag_id) DO UPDATE SET
            start_time=excluded.start_time, end_time=excluded.end_time,
            rows_extracted=excluded.rows_extracted, rows_loaded=excluded.rows_loaded,
            rows_quarantined=excluded.rows_quarantined, status=excluded.status
        """,
        [
            run_id, dag_id, start_time, datetime.now(timezone.utc),
            rows_extracted, rows_loaded, rows_quarantined, status,
        ],
    )
    con.close()
