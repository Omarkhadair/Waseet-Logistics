"""Data quality suite, schema drift, and reconciliation for Waseet Logistics."""

import argparse
import json
import os
import sys
import pandas as pd
import sqlalchemy

DATA = "/opt/airflow/data"
CODE = "/opt/airflow/pipeline"
BASELINE_PATH = os.path.join(CODE, "schema_baseline.json")

SUITE = [
    {"column": "scan_id", "check": "not_null", "dimension": "Completeness"},
    {"column": "scan_id", "check": "unique", "dimension": "Uniqueness"},
    {"column": "parcel_id", "check": "not_null", "dimension": "Completeness"},
    {"column": "scanned_at", "check": "not_null", "dimension": "Completeness"},
    {"column": "scanned_at", "check": "date_iso", "dimension": "Validity"},
    {"column": "scan_type", "check": "not_null", "dimension": "Completeness"},
    {
        "column": "scan_type",
        "check": "in_set",
        "dimension": "Consistency",
        "allowed": ["pickup", "transit", "delivered", "exception"],
    },
    {"column": "station_id", "check": "not_null", "dimension": "Completeness"},
]


def run_check(df, rule):
    column_name = rule["column"]
    check_type = rule["check"]

    if column_name not in df.columns:
        return len(df)

    col = df[column_name]

    if check_type == "not_null":
        bad = col.isna() | (col.astype(str).str.strip() == "")
    elif check_type == "unique":
        bad = col.duplicated()
    elif check_type == "numeric":
        bad = pd.to_numeric(col, errors="coerce").isna() & col.notna()
    elif check_type == "positive":
        bad = pd.to_numeric(col, errors="coerce") <= 0
    elif check_type == "date_iso":
        parsed = pd.to_datetime(col, errors="coerce")
        bad = parsed.isna() & col.notna()
    elif check_type == "in_set":
        allowed = [a.lower() for a in rule.get("allowed", [])]
        bad = ~col.astype(str).str.lower().isin(allowed) & col.notna()
    else:
        raise ValueError(f"Unknown check type: {check_type}")

    return int(bad.sum())


def validate(df, suite):
    results = []
    for rule in suite:
        failed = run_check(df, rule)
        results.append(
            {
                "column": rule["column"],
                "check": rule["check"],
                "dimension": rule.get("dimension", "Unknown"),
                "failed": failed,
                "passed": failed == 0,
            }
        )
    return pd.DataFrame(results)


def check_schema_drift(df):
    if not os.path.exists(BASELINE_PATH):
        baseline = {"columns": list(df.columns)}
        with open(BASELINE_PATH, "w") as f:
            json.dump(baseline, f)

    with open(BASELINE_PATH) as f:
        expected = json.load(f)["columns"]

    current = list(df.columns)
    missing = [c for c in expected if c not in current]
    new_cols = [c for c in current if c not in expected]
    return missing, new_cols


def reconcile(date_str):
    file_path = f"{DATA}/scans_{date_str}.csv"
    if not os.path.exists(file_path):
        return None, None, "File missing"

    df = pd.read_csv(file_path, dtype=str)
    in_file = len(df)

    db_url = os.environ.get(
        "WASEET_DB_URL", "postgresql+psycopg2://de:de@waseet-postgres:5432/waseet"
    )
    engine = sqlalchemy.create_engine(db_url)
    with engine.connect() as conn:
        res = conn.execute(
            sqlalchemy.text(
                "SELECT count(*) FROM parcel_scans WHERE DATE(scanned_at) = :d"
            ),
            {"d": date_str},
        )
        loaded = res.scalar()

    return in_file, loaded, None


def main():
    parser = argparse.ArgumentParser(description="Waseet Quality Suite")
    parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
    args = parser.parse_args()

    date_str = args.date
    file_path = f"{DATA}/scans_{date_str}.csv"

    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        sys.exit(1)

    day = pd.read_csv(file_path, dtype=str)

    missing, new_cols = check_schema_drift(day)
    if missing:
        print(f"CRITICAL: Missing columns compared to baseline: {missing}")
        sys.exit(1)
    if new_cols:
        print(f"WARNING: New columns detected: {new_cols}")

    report = validate(day, SUITE)
    print("Quality Suite Report:")
    print(report.to_string(index=False))

    failures = report[~report["passed"]]
    if not failures.empty:
        print(f"Quality suite failed on {len(failures)} rules.")
        sys.exit(1)

    in_file, loaded, err = reconcile(date_str)
    if err:
        print(f"Reconciliation notice: {err}")
    else:
        print(
            f"Reconciliation for {date_str} -> In File: {in_file}, Loaded in Warehouse: {loaded}"
        )

    print("Quality and reconciliation completed successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
