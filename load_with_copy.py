#!/usr/bin/env python3
"""
Load parsed CSV into PostgreSQL using COPY and a TEXT-only staging table.

Environment variables:
    PGHOST
    PGPORT
    PGUSER
    PGPASSWORD
    PGDATABASE

Input CSV:
    ./parsed/log_entry.csv
"""

import os
import psycopg

from datetime import datetime, timezone

CSV_PATH = "./parsed/log_entry.csv"

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{ts} | {msg}", flush=True)


# ======================================================
# COPY into staging (TEXT columns)
# ======================================================
def copy_into_staging(conn) -> None:
    log("== COPY INTO STAGING ==")

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    with conn.cursor() as cur:
        log("Truncating staging table...")
        cur.execute("TRUNCATE TABLE log_entry_staging;")
        conn.commit()

    copy_sql = """
        COPY log_entry_staging (
            log_type_name,
            action_type_name,
            log_timestamp,
            source_ip,
            dest_ip,
            block_id,
            size_bytes,
            detail
        )
        FROM STDIN WITH (FORMAT csv, HEADER true)
    """

    log("Copying CSV into staging...")
    with conn.cursor() as cur, open(CSV_PATH, "r", encoding="utf-8") as f:
        with cur.copy(copy_sql) as copy:
            copy.write(f.read())

    conn.commit()
    log("COPY completed.\n")


# ======================================================
# Insert missing action types
# ======================================================
def populate_action_types(conn) -> None:
    log("== POPULATE action_type ==")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO action_type (name)
            SELECT DISTINCT action_type_name
            FROM log_entry_staging
            WHERE action_type_name IS NOT NULL
              AND action_type_name <> ''
            ON CONFLICT (name) DO NOTHING;
            """
        )
    conn.commit()

    log("action_type mapped.\n")


# ======================================================
# Move data from staging → final table
# ======================================================
def move_from_staging_to_final(conn) -> int:
    log("== INSERT INTO log_entry ==")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO log_entry (
                log_type_id,
                action_type_id,
                log_timestamp,
                source_ip,
                dest_ip,
                block_id,
                size_bytes,
                detail
            )
            SELECT
                lt.id AS log_type_id,
                at.id AS action_type_id,

                -- timestamp
                s.log_timestamp::timestamptz,

                -- source_ip
                CASE
                    WHEN s.source_ip IS NULL OR s.source_ip = '' THEN NULL
                    ELSE s.source_ip::inet
                END,

                -- dest_ip
                CASE
                    WHEN s.dest_ip IS NULL OR s.dest_ip = '' THEN NULL
                    ELSE s.dest_ip::inet
                END,

                -- block_id
                CASE
                    WHEN s.block_id IS NULL OR s.block_id = '' THEN NULL
                    ELSE s.block_id::bigint
                END,

                -- size_bytes
                CASE
                    WHEN s.size_bytes IS NULL OR s.size_bytes = '' THEN NULL
                    ELSE s.size_bytes::bigint
                END,

                -- detail (JSONB)
                CASE
                    WHEN s.detail IS NULL OR s.detail = '' THEN '{}'::jsonb
                    ELSE s.detail::jsonb
                END

            FROM log_entry_staging AS s
            JOIN log_type lt ON lt.name = s.log_type_name
            LEFT JOIN action_type at ON at.name = s.action_type_name;
            """
        )
        inserted = cur.rowcount

    conn.commit()
    log(f"Inserted {inserted} rows.\n")
    return inserted


# ======================================================
# Main
# ======================================================
def main() -> None:
    log("Connecting to PostgreSQL...")

    conn = psycopg.connect(
        dbname=os.getenv("PGDATABASE", "logdb"),
        user=os.getenv("PGUSER", "admin"),
        password=os.getenv("PGPASSWORD", "admin123!"),
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
    )

    try:
        copy_into_staging(conn)
        populate_action_types(conn)
        inserted = move_from_staging_to_final(conn)

        log("== DONE ==")
        log(f"Final rows inserted: {inserted}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
