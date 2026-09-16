"""Waseet Logistics daily scan pipeline."""

print("pipeline.py is running")
import argparse
import logging
import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text

DATA_DIR = os.environ.get("WASEET_DATA_DIR", "../data")
DB_URL = os.environ.get("WASEET_DB_URL",
                        "postgresql+psycopg2://de:de@localhost:5442/waseet")

REQUIRED = ["scan_id", "parcel_id", "hub_id", "scan_type", "scanned_at", "weight_kg"]
COLUMNS = ["scan_id", "parcel_id", "hub_id", "scan_type", "scanned_at", "weight_kg", "courier_id", "customer_id", "service_level_id"]

os.makedirs("logs", exist_ok=True)
os.makedirs("quarantine", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/pipeline.log"), logging.StreamHandler()])
log = logging.getLogger("waseet")

engine = create_engine(DB_URL)


def extract(scan_date):
    path = os.path.join(DATA_DIR, f"scans_{scan_date}.csv")
    df = pd.read_csv(path, dtype=str)
    
    if "timestamp" in df.columns and "scanned_at" not in df.columns:
        df = df.rename(columns={"timestamp": "scanned_at"})
    if "id" in df.columns and "scan_id" not in df.columns:
        df = df.rename(columns={"id": "scan_id"})
    log.info("extract: %d rows from %s", len(df), path)
    return df


def check_contract(df):
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError("missing columns: " + str(missing))
    if len(df) == 0:
        raise ValueError("file is empty")
    log.info("contract: ok, %d columns", len(df.columns))


def parse_timestamps(series):
    iso = pd.to_datetime(series, format="ISO8601", errors="coerce")
    if iso.isna().all():
        iso = pd.to_datetime(series, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    alt = pd.to_datetime(series, format="%d/%m/%Y %H:%M", errors="coerce")
    return iso.fillna(alt)


def transform(raw, scan_date, hubs, couriers, customers, services):
    df = raw.copy()
    
    for col in ["courier_id", "customer_id", "service_level_id"]:
        if col not in df.columns:
            df[col] = None

    initial_count = len(df)
    df = df.drop_duplicates(subset=["scan_id"], keep="first")
    duplicates_count = initial_count - len(df)
    df["scanned_at"] = parse_timestamps(df["scanned_at"])
    df["weight_kg"] = df["weight_kg"].astype(str).str.replace(",", ".", regex=False)
    df["weight_kg"] = pd.to_numeric(df["weight_kg"], errors="coerce")
    df["scan_type"] = df["scan_type"].str.lower().str.strip()

    df["reject_reason"] = None
    df.loc[df["scanned_at"].isna(), "reject_reason"] = "unparseable timestamp"
    df.loc[df["weight_kg"].isna() | (df["weight_kg"] <= 0) | (df["weight_kg"] > 500), "reject_reason"] = "bad weight"
    df.loc[df["hub_id"].astype(str) == "99", "reject_reason"] = "invalid hub 99"
    df.loc[~df["hub_id"].astype(str).isin(hubs["hub_id"].astype(str)), "reject_reason"] = "unknown hub"

    good = df[df["reject_reason"].isna()].copy()
    rejects = df[df["reject_reason"].notna()]

    if len(rejects) > 0:
        rejects.to_csv(f"quarantine/rejects_{scan_date}.csv", index=False)
        log.warning("quarantined %d rows to quarantine/rejects_%s.csv", len(rejects), scan_date)

    good["hub_id"] = good["hub_id"].astype(str)
    good["weight_kg"] = good["weight_kg"].round(2)

    log.info("transform: %d read = %d good + %d rejected + %d duplicates",
             initial_count, len(good), len(rejects), duplicates_count)
    
    return good, rejects, duplicates_count


def load(good, scan_date):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM parcel_scans WHERE DATE(scanned_at) = :d"), {"d": scan_date})
        if not good.empty:
            good.to_sql("parcel_scans", conn, if_exists="append", index=False)
    log.info("load: %d rows for %s", len(good), scan_date)
    return len(good)


def write_load_log(scan_date, rows_read, rows_loaded, rows_rejected, duplicates_count, status, error_message=None):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO load_log (scan_date, rows_read, rows_loaded, rows_rejected, rows_deduplicated, status, error_message, executed_at)
            VALUES (:scan_date, :rows_read, :rows_loaded, :rows_rejected, :rows_deduplicated, :status, :error_message, NOW())
        """), {
            "scan_date": scan_date,
            "rows_read": rows_read,
            "rows_loaded": rows_loaded,
            "rows_rejected": rows_rejected,
            "rows_deduplicated": duplicates_count,
            "status": status,
            "error_message": error_message
        })
         
def main(scan_date):
    log.info("run starting for %s", scan_date)
    try:
        raw = extract(scan_date)
        check_contract(raw)
        hubs = pd.read_csv(DATA_DIR + "/hubs.csv")
        couriers = pd.read_csv(DATA_DIR + "/couriers.csv")
        services = pd.read_csv(DATA_DIR + "/service_levels.csv")
        customers = pd.read_sql_query("SELECT customer_id FROM customers", engine)
        
        good, rejects, duplicates_count = transform(raw, scan_date, hubs, couriers, customers, services)
        load(good, scan_date)
        
        rows_read = len(raw)
        rows_loaded = len(good)
        rows_rejected = len(rejects)
        
        write_load_log(scan_date, rows_read, rows_loaded, rows_rejected, duplicates_count, "SUCCESS")
        log.info("run finished for %s", scan_date)
        return 0
    except Exception as problem:
        log.error("run FAILED for %s: %s", scan_date, problem)
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    sys.exit(main(parser.parse_args().date))