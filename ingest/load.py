#!/usr/bin/env python3
"""
Load parsed CSV files into PostgreSQL using COPY.

Environment variables:
  PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE

Input CSVs:
  ./parsed/log_type.csv
  ./parsed/action_type.csv
  ./parsed/log_entry.csv
  ./parsed/log_access_detail.csv
"""

import os
from typing import List

import psycopg
from psycopg import Connection

from util import tiny_logger


CSV_DIR = "./parsed"

LOG_TYPE_CSV = os.path.join(CSV_DIR, "log_type.csv")
ACTION_TYPE_CSV = os.path.join(CSV_DIR, "action_type.csv")
LOG_ENTRY_CSV = os.path.join(CSV_DIR, "log_entry.csv")
ACCESS_DETAIL_CSV = os.path.join(CSV_DIR, "log_access_detail.csv")


def copy_csv(
    conn: Connection,
    table: str,
    csv_path: str,
    columns: List[str],
) -> int:
    """
    Load a CSV file into a PostgreSQL table using COPY.

    :param conn: psycopg connection.
    :param table: Target PostgreSQL table name.
    :param csv_path: Path to the CSV file.
    :param columns: Ordered column names matching the CSV header.
    :return: Number of inserted rows.
    :raises FileNotFoundError: If the CSV file does not exist.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing CSV: {csv_path}")

    collist = ", ".join(columns)
    copy_sql = (
        f"COPY {table} ({collist}) "
        "FROM STDIN WITH (FORMAT csv, HEADER true)"
    )

    tiny_logger(f"COPY → {table} ({collist}) from {csv_path}")

    with conn.cursor() as cur, open(csv_path, "r", encoding="utf-8") as infile:
        with cur.copy(copy_sql) as cp:
            cp.write(infile.read())
        rowcount = cur.rowcount

    conn.commit()
    return rowcount


def truncate_all(conn: Connection) -> None:
    """
    Truncate all target tables and reset identity sequences.

    :param conn: psycopg connection.
    :return: None
    """
    tiny_logger("Truncating all target tables...")
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE log_access_detail, log_entry, "
            "log_type, action_type RESTART IDENTITY;"
        )
    conn.commit()
    tiny_logger("Truncate complete.\n")


def main() -> None:
    """
    Execute the ingestion pipeline.

    Steps:
      1. Connect to PostgreSQL.
      2. Truncate tables.
      3. COPY log_type.
      4. COPY action_type.
      5. COPY log_entry.
      6. COPY log_access_detail.

    Connection parameters are obtained from environment variables.
    """
    tiny_logger("Connecting to PostgreSQL...")

    conn = psycopg.connect(
        dbname=os.getenv("PGDATABASE", "logdb"),
        user=os.getenv("PGUSER", "admin"),
        password=os.getenv("PGPASSWORD", "admin123!"),
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
    )

    try:
        truncate_all(conn)

        copy_csv(conn, "log_type", LOG_TYPE_CSV, ["id", "name"])
        copy_csv(conn, "action_type", ACTION_TYPE_CSV, ["id", "name"])

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
                "size_bytes",
            ],
        )

        copy_csv(
            conn,
            "log_access_detail",
            ACCESS_DETAIL_CSV,
            [
                "log_entry_id",
                "remote_name",
                "auth_user",
                "resource",
                "http_status",
                "referrer",
                "user_agent",
            ],
        )

        tiny_logger("== DONE ==")
        tiny_logger("All tables loaded successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
