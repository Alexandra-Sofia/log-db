#!/usr/bin/env python3
"""
Ingest pipeline entrypoint.

This module performs a full ingestion cycle consisting of:

1. Parsing raw log files from ``INPUT_DIR`` using ``run_parser``.
2. Establishing a PostgreSQL connection using environment variables.
3. Inserting the parsed rows into the database via ``insert_logs`` with
   batched ingestion.
4. Reporting progress and errors through ``tiny_logger``.

The script is designed to run non-interactively inside Docker Compose.
"""

import os
import psycopg2
from typing import Dict, List, Any

from parser import parse_access, parse_dataxceiver, parse_namesystem
from loader import load
from util import tiny_logger, LogType

INPUT_DIR = "/input-logfiles"

PARSERS = {
    LogType.ACCESS: parse_access,
    LogType.HDFS_DATAXCEIVER: parse_dataxceiver,
    LogType.HDFS_NAMESYSTEM: parse_namesystem,
}

def parse() -> Dict[str, List[Dict[str, Any]]]:
    result = {}
    for lt in LogType:
        parser = PARSERS[lt]
        path = os.path.join(INPUT_DIR, lt.filename)
        tiny_logger(f"Parsing {path} ...")
        result[lt.value] = parser(path)
    return result

def main():
    """
    Execute the full ingestion workflow.

    The workflow consists of parsing, database connection, and
    uploading parsed rows into the ``log_entry`` and associated tables.
    Progress and errors are logged via ``tiny_logger``.
    """
    tiny_logger("=== INGEST START ===")

    tiny_logger("Parsing input-logfiles...")
    try:
        parsed = parse()
    except Exception as exc:
        tiny_logger(f"Parsing failed: {exc}")
        raise

    stats = {
        lt.value: len(parsed.get(lt.value, []))
        for lt in LogType
    }

    tiny_logger(f"Parse stats: {stats}")

    tiny_logger("Connecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("PGDATABASE", "logdb"),
            user=os.getenv("PGUSER", "admin"),
            password=os.getenv("PGPASSWORD", "admin123!"),
            host=os.getenv("PGHOST", "postgres"),
            port=os.getenv("PGPORT", "5432"),
        )
        tiny_logger("DB connection OK.")
    except Exception as exc:
        tiny_logger(f"Failed to connect to DB: {exc}")
        raise

    tiny_logger("Uploading parsed logs to DB...")
    try:
        load(conn, parsed)
    except Exception as exc:
        tiny_logger(f"Upload failed: {exc}")
        raise
    finally:
        conn.close()

    tiny_logger("=== INGEST COMPLETE ===")


if __name__ == "__main__":
    main()
