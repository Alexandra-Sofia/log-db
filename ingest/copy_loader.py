#!/usr/bin/env python3
"""
Load parsed CSVs into PostgreSQL using direct COPY into final tables.

Environment variables:
    PGHOST
    PGPORT
    PGUSER
    PGPASSWORD
    PGDATABASE

Input CSVs:
    ./parsed/log_type.csv
    ./parsed/action_type.csv
    ./parsed/log_entry.csv
    ./parsed/log_access_detail.csv
"""

import os
import psycopg
from datetime import datetime, timezone

# CSV paths
CSV_DIR = "./parsed"

LOG_TYPE_CSV = os.path.join(CSV_DIR, "log_type.csv")
ACTION_TYPE_CSV = os.path.join(CSV_DIR, "action_type.csv")
LOG_ENTRY_CSV = os.path.join(CSV_DIR, "log_entry.csv")
ACCESS_DETAIL_CSV = os.path.join(CSV_DIR, "log_access_detail.csv")


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{ts} | {msg}", flush=True)


# ======================================================
# Generic COPY helper
# ======================================================
def copy_csv(conn, table: str, csv_path: str, columns: list[str]) -> int:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing CSV: {csv_path}")

    collist = ", ".join(columns)
    copy_sql = f"""
        COPY {table} ({collist})
        FROM STDIN WITH (FORMAT csv, HEADER true)
    """

    log(f"COPY → {table} ({collist}) from {csv_path}")
    with conn.cursor() as cur, open(csv_path, "r", encoding="utf-8") as f:
        with cur.copy(copy_sql) as cp:
            cp.write(f.read())
        rowcount = cur.rowcount

    conn.commit()
    return rowcount


# ======================================================
# Truncate all target tables before load
# ======================================================
def truncate_all(conn):
    log("Truncating all target tables...")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE log_access_detail, log_entry, log_type, action_type RESTART IDENTITY;")
    conn.commit()
    log("Truncate complete.\n")


# ======================================================
# Main loader
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
        truncate_all(conn)

        # 1. Load log_type (id, name)
        copy_csv(
            conn,
            "log_type",
            LOG_TYPE_CSV,
            ["id", "name"]
        )

        # 2. Load action_type (id, name)
        copy_csv(
            conn,
            "action_type",
            ACTION_TYPE_CSV,
            ["id", "name"]
        )

        # 3. Load log_entry
        copy_csv(
            conn,
            "log_entry",
            LOG_ENTRY_CSV,
            [
                "id",
                "log_type_id",
                "action_type_id",
                "log_timestamp",
                "source_ip",
                "dest_ip",
                "block_id",
                "size_bytes"
            ]
        )

        # 4. Load access detail
        copy_csv(
            conn,
            "log_access_detail",
            ACCESS_DETAIL_CSV,
            [
                "log_entry_id",
                "remote_name",
                "auth_user",
                "http_method",
                "resource",
                "http_status",
                "referrer",
                "user_agent"
            ]
        )

        log("== DONE ==")
        log("All tables loaded successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
