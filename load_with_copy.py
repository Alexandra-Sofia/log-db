#!/usr/bin/env python3
"""
Load parsed CSV into PostgreSQL using COPY and staging table.

Environment variables:
    PGHOST
    PGPORT
    PGUSER
    PGPASSWORD
    PGDATABASE

Input:
    ./parsed/log_entry.csv
"""

import os
import psycopg
from psycopg import sql

CSV_PATH = "./parsed/log_entry.csv"


# ======================================================
# COPY into staging using psycopg3 COPY API
# ======================================================
def copy_into_staging(conn) -> None:
    """
    COPY CSV into log_entry_staging using psycopg3's COPY context.
    """
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    print("  - Truncating staging table...")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE log_entry_staging;")
        conn.commit()

    print("  - Executing COPY into staging...")

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

    # psycopg3 COPY usage
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                copy.write(f.read())

    conn.commit()
    print("  - COPY into staging completed.")


# ======================================================
# Insert missing action types
# ======================================================
def populate_action_types(conn) -> None:
    print("  - Populating action_type lookup table...")
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
    print("  - action_type updated.")


# ======================================================
# Move data from staging to final table
# ======================================================
def move_from_staging_to_final(conn) -> int:
    print("  - Moving rows from staging to final table...")

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
                s.log_timestamp,
                NULLIF(s.source_ip, '')::inet,
                NULLIF(s.dest_ip, '')::inet,
                NULLIF(s.block_id::text, '')::bigint,
                NULLIF(s.size_bytes::text, '')::bigint,
                s.detail
            FROM log_entry_staging AS s
            JOIN log_type lt
              ON lt.name = s.log_type_name
            LEFT JOIN action_type at
              ON at.name = s.action_type_name
            ;
            """
        )
        inserted = cur.rowcount

    conn.commit()
    print(f"  - Moved {inserted} rows.")
    return inserted


# ======================================================
# Main
# ======================================================
def main() -> None:
    dsn = os.getenv("PGDATABASE", "logdb")
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    user = os.getenv("PGUSER", "admin")
    password = os.getenv("PGPASSWORD", "admin123!")

    print("Connecting to PostgreSQL...")
    conn = psycopg.connect(
        dbname=dsn,
        user=user,
        password=password,
        host=host,
        port=port,
    )

    try:
        print("COPY into staging...")
        copy_into_staging(conn)

        print("Populating action_type...")
        populate_action_types(conn)

        print("Loading final table log_entry...")
        inserted = move_from_staging_to_final(conn)

        print(f"Done. Inserted {inserted} rows into log_entry.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
