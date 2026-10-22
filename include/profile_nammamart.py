"""Generate a PII-safe profile of the six supplied NammaMart CSV files."""

import json
from pathlib import Path

import pandas as pd

from nammamart_pipeline import DATA_DIR, INCREMENT_FILE, SOURCE_FILES


KEY_COLUMNS = {
    "orders": "order_id",
    "orders_2026-10-01_increment": "order_id",
    "payments": "payment_id",
    "customers": "customer_id",
    "products": "product_id",
    "stores": "store_id",
}
NUMERIC_COLUMNS = {
    "orders": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
    "payments": ["amount_paid"],
    "customers": ["pincode"],
    "products": ["mrp", "cost_price"],
    "stores": ["pincode", "capacity_orders_per_day"],
}
CATEGORY_COLUMNS = {
    "orders": ["order_status", "payment_mode"],
    "payments": ["payment_status", "payment_mode"],
    "customers": ["loyalty_tier"],
    "products": ["category"],
    "stores": ["zone"],
}
RELATIONSHIPS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("orders", "product_id", "products", "product_id"),
    ("orders", "store_id", "stores", "store_id"),
    ("payments", "order_id", "orders", "order_id"),
]


def _read(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def build_profile() -> dict:
    files = dict(SOURCE_FILES)
    files["orders_2026-10-01_increment"] = INCREMENT_FILE
    frames = {
        name: _read(DATA_DIR / filename)
        for name, filename in files.items()
    }
    profiles: dict[str, dict] = {}
    for name, frame in frames.items():
        key = KEY_COLUMNS[name]
        schema_name = "orders" if name.startswith("orders_") else name
        duplicate_mask = frame[key].duplicated(keep=False)
        duplicate_key_values = sorted(
            frame.loc[duplicate_mask, key].astype(str).unique().tolist()
        )
        if name == "customers":
            duplicate_key_values = [
                f"customer_duplicate_{index:02d}"
                for index in range(1, len(duplicate_key_values) + 1)
            ]
        numeric_ranges = {}
        for column in NUMERIC_COLUMNS.get(schema_name, []):
            values = pd.to_numeric(frame[column], errors="coerce")
            numeric_ranges[column] = {
                "min": None if values.notna().sum() == 0 else float(values.min()),
                "max": None if values.notna().sum() == 0 else float(values.max()),
                "invalid_or_blank": int(values.isna().sum()),
            }
        date_ranges = {}
        for column in (col for col in frame if col.endswith("_date") or col.endswith("_ts")):
            values = pd.to_datetime(frame[column], errors="coerce")
            date_ranges[column] = {
                "min": None if values.notna().sum() == 0 else str(values.min()),
                "max": None if values.notna().sum() == 0 else str(values.max()),
                "invalid_or_blank": int(values.isna().sum()),
            }
        profiles[name] = {
            "source_file": files[name],
            "rows": int(len(frame)),
            "columns": list(frame.columns),
            "blank_values_by_column": {
                column: int(frame[column].astype(str).str.strip().eq("").sum())
                for column in frame
                if frame[column].astype(str).str.strip().eq("").any()
            },
            "duplicate_key_rows": int(duplicate_mask.sum()),
            "duplicate_key_values": duplicate_key_values,
            "duplicate_key_values_redacted": name == "customers",
            "numeric_ranges": numeric_ranges,
            "date_ranges": date_ranges,
            "distinct_categories": {
                column: sorted(frame[column].astype(str).str.strip().unique().tolist())
                for column in CATEGORY_COLUMNS.get(schema_name, [])
            },
        }

    link_results = []
    for source, source_key, target, target_key in RELATIONSHIPS:
        source_frame = frames[source]
        target_ids = set(frames[target][target_key].astype(str).str.strip())
        missing = source_frame[source_key].astype(str).str.strip().eq("")
        unknown = ~source_frame[source_key].astype(str).str.strip().isin(target_ids)
        if source == "payments":
            target_ids.update(frames["orders_2026-10-01_increment"]["order_id"].astype(str))
            unknown = ~source_frame[source_key].astype(str).str.strip().isin(target_ids)
        link_results.append({
            "relationship": f"{source}.{source_key} -> {target}.{target_key}",
            "rows_checked": int(len(source_frame)),
            "missing_keys": int(missing.sum()),
            "unknown_keys": int((unknown & ~missing).sum()),
        })

    issue_path = DATA_DIR.parent.parent / "profile" / "nammamart_data_issues.csv"
    issue_summary = []
    if issue_path.exists():
        issue_frame = pd.read_csv(issue_path, dtype=str, keep_default_na=False)
        if not issue_frame.empty:
            issue_summary = (
                issue_frame.groupby(
                    ["file", "quality_dimension", "severity", "problem"],
                    dropna=False,
                )
                .size()
                .rename("issue_count")
                .reset_index()
                .to_dict("records")
            )
    return {
        "profiled_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "data_directory": str(DATA_DIR),
        "total_rows_including_increment": int(sum(
            profile["rows"] for profile in profiles.values()
        )),
        "files": profiles,
        "referential_integrity": link_results,
        "quality_issue_summary": issue_summary,
    }


if __name__ == "__main__":
    output_path = DATA_DIR.parent.parent / "profile" / "nammamart_profile.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(build_profile(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"Wrote PII-safe profile: {output_path}")
