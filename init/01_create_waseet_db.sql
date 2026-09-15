-- Waseet Logistics warehouse.
--
-- This file runs once, the first time the postgres container starts on an empty
-- volume. If you change it afterwards, nothing happens until you run
-- `docker compose down -v` and bring the stack back up.
--
-- Four dimensions, one fact table, one run log. The dimensions are given. The
-- fact table and the log are yours to finish - see the TODOs.
DROP TABLE IF EXISTS load_log;

DROP TABLE IF EXISTS parcel_scans;

DROP TABLE IF EXISTS customers;

DROP TABLE IF EXISTS couriers;

DROP TABLE IF EXISTS service_levels;

DROP TABLE IF EXISTS hubs;

CREATE TABLE hubs (
    hub_id   INT  PRIMARY KEY,
    hub_name TEXT NOT NULL,
    city     TEXT NOT NULL,
    country  TEXT NOT NULL,
    region   TEXT NOT NULL
);

CREATE TABLE service_levels (
    service_code   TEXT PRIMARY KEY,
    service_name   TEXT NOT NULL,
    promised_hours INT  NOT NULL
);

CREATE TABLE couriers (
    courier_id   INT  PRIMARY KEY,
    courier_name TEXT NOT NULL,
    hub_id       INT  NOT NULL FOREIGN KEY REFERENCES hubs (hub_id),
    vehicle_type TEXT NOT NULL
);

-- Loaded from the API, not from a file. It is a dimension like any other once
-- it lands; where it came from stops mattering at the warehouse boundary.
CREATE TABLE customers (
    customer_id   INT             PRIMARY KEY,
    customer_name TEXT            NOT NULL,
    segment       TEXT            NOT NULL,
    city          TEXT           ,
    signup_date   DATE           ,
    credit_limit  NUMERIC (12, 2)
);

-- TODO (yours). The scan events.
--
-- Decisions to make, and to be able to defend:
--
--   * the grain - what does one row mean? One scan event, or something else?
--   * which columns are NOT NULL. A scan with no courier is a real thing in this
--     feed. A scan with no timestamp is not.
--   * which foreign keys to declare. A missing hub should not reach this table,
--     but an FK is not the only way to stop it, and it is not free.
--   * whether scan_id is a primary key. Read the note on the Najm `sales` table
--     in Lecture 13 before you decide - the answer there was no, on purpose, and
--     the reason was not laziness.
--   * which index the daily load and the daily report actually need.
--
CREATE TABLE parcel_scans (
    scan_id      TEXT           NOT NULL,
    parcel_id    TEXT           NOT NULL,
    customer_id  INT            NOT NULL FOREIGN KEY REFERENCES customers (customer_id),
    scanned_at   TIMESTAMP      NOT NULL,
    hub_id       INT            FOREIGN KEY REFERENCES hubs (hub_id),
    courier_id   INT            FOREIGN KEY REFERENCES couriers (courier_id),
    scan_type    TEXT           NOT NULL,
    weight_kg    NUMERIC (8, 2) NOT NULL,
    service_code TEXT           FOREIGN KEY REFERENCES service_levels (service_code)
);

-- TODO (yours). The run log. Lecture 16 section 5 shows the shape: one row per
-- run, with what it read, what it loaded and what it rejected. Nothing about the
-- data itself goes in here.
--
CREATE TABLE load_log (
    run_id        SERIAL    PRIMARY KEY,
    run_timestamp TIMESTAMP NOT NULL,
    run_ended     TIMESTAMP NOT NULL,
    rows_read     INT       NOT NULL,
    rows_loaded   INT       NOT NULL,
    rows_rejected INT       NOT NULL
);