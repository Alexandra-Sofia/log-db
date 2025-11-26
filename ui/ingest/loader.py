from typing import Dict, List, Any, Tuple
import psycopg2
from psycopg2.extras import execute_values

from ..util.logger import logger

BATCH_SIZE = 5000


# ------------------------------------------------------------
# Helpers to work with small dimension tables
# ------------------------------------------------------------
def get_log_type_ids(conn) -> Dict[str, int]:
    """
    Load all log_type rows and return a mapping name -> id.
    """
    logger.info("Fetching log_type IDs from database...")
    mapping: Dict[str, int] = {}

    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM log_type;")
        for id_, name in cur.fetchall():
            mapping[name.upper()] = id_

    logger.info(f"Loaded {len(mapping)} log types: {mapping}")
    return mapping


def get_action_type_id(conn, name: str | None) -> int | None:
    """
    Ensure an action_type row exists for the given name, returning its id.
    If name is None or empty, returns None.
    """
    if not name:
        return None

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM action_type WHERE name = %s;", (name,))
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            "INSERT INTO action_type (name) VALUES (%s) RETURNING id;",
            (name,),
        )
        new_id = cur.fetchone()[0]
        logger.info(f"Created new action_type: {name} -> id {new_id}")
        return new_id
    return None


# ------------------------------------------------------------
# Top-level ingestion entry point
# ------------------------------------------------------------
def insert_logs(conn, parsed: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Insert all parsed logs into the database.

    parsed keys are expected to be:
      'ACCESS', 'HDFS_DATAXCEIVER', 'HDFS_NAMESYSTEM'

    Each value is a list of dict rows produced by your parser.
    """
    logger.info("Beginning log ingestion process...")
    logger.info(f"Parsed log types present: {list(parsed.keys())}")

    log_type_ids = get_log_type_ids(conn)

    with conn:
        with conn.cursor() as cur:
            total_rows = 0

            for key, rows in parsed.items():
                lt_name = key.upper()
                if lt_name not in log_type_ids:
                    logger.warning(f"Skipping unknown log_type {lt_name}.")
                    continue

                lt_id = log_type_ids[lt_name]
                logger.info(f"Preparing {len(rows)} rows for type '{lt_name}'...")

                inserted_for_type = _insert_for_log_type(conn, cur, lt_id, rows)
                total_rows += inserted_for_type
                logger.info(f"Inserted {inserted_for_type} rows for '{lt_name}'.")

            logger.info(f"FINAL: total inserted into log_entry = {total_rows}")


# ------------------------------------------------------------
# Per-type ingestion with batching
# ------------------------------------------------------------
def _insert_for_log_type(conn, cur, log_type_id: int, rows: List[Dict[str, Any]]) -> int:
    """
    Insert all rows of a specific log type in batches.
    """
    entry_batch: List[Tuple[Any, ...]] = []
    detail_staging: List[Tuple[int, Dict[str, Any]]] = []
    inserted_count = 0

    for row in rows:
        action_name = row.get("action_type_name")
        action_id = get_action_type_id(conn, action_name)

        idx = len(entry_batch)

        # NEW entry tuple — matches updated schema
        entry_batch.append(
            (
                log_type_id,
                action_id,
                row.get("log_timestamp"),
                row.get("source_ip"),
                row.get("dest_ip"),
                row.get("block_id"),
                row.get("size_bytes"),
            )
        )

        if row.get("detail"):
            detail_staging.append((idx, row["detail"]))

        if len(entry_batch) >= BATCH_SIZE:
            inserted = _flush_entry_batch(cur, entry_batch, detail_staging)
            inserted_count += inserted
            entry_batch.clear()
            detail_staging.clear()

    # final flush
    if entry_batch:
        inserted = _flush_entry_batch(cur, entry_batch, detail_staging)
        inserted_count += inserted

    return inserted_count


# ------------------------------------------------------------
# Flush batch using psycopg2.extras.execute_values
# ------------------------------------------------------------
def _flush_entry_batch(cur, entry_batch, detail_staging) -> int:
    """
    Insert a batch of log_entry rows and the matching log_access_detail rows.
    """
    if not entry_batch:
        return 0

    logger.info(f"Flushing batch of {len(entry_batch)} rows...")

    entry_sql = """
        INSERT INTO log_entry (
            log_type_id,
            action_type_id,
            log_timestamp,
            source_ip,
            dest_ip,
            block_id,
            size_bytes
        )
        VALUES %s
        RETURNING id;
    """

    try:
        returned = execute_values(cur, entry_sql, entry_batch, fetch=True)
        entry_ids = [r[0] for r in returned]
    except Exception as e:
        logger.error("Failed to insert log_entry batch.")
        logger.exception(e)
        raise

    logger.info(f"Inserted {len(entry_ids)} log_entry rows.")

    if detail_staging:
        logger.info(f"Inserting {len(detail_staging)} ACCESS detail rows...")

        detail_vals = []
        for idx, detail in detail_staging:
            detail_vals.append(
                (
                    entry_ids[idx],
                    detail.get("remote_name"),
                    detail.get("auth_user"),
                    detail.get("http_method"),
                    detail.get("resource"),
                    detail.get("http_status"),
                    detail.get("referrer"),
                    detail.get("user_agent"),
                )
            )

        detail_sql = """
            INSERT INTO log_access_detail (
                log_entry_id,
                remote_name,
                auth_user,
                http_method,
                resource,
                http_status,
                referrer,
                user_agent
            )
            VALUES %s;
        """

        execute_values(cur, detail_sql, detail_vals)
        logger.info("ACCESS detail insertion complete.")

    return len(entry_ids)
