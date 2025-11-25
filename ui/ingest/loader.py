import psycopg
from typing import Dict, List, Any
from ..util.logger import logger

BATCH_SIZE = 50000


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def get_log_type_ids(conn) -> Dict[str, int]:
    """
    Fetch all log types from the database.
    Return a mapping: {"ACCESS": 1, "HDFS_DATAXCEIVER": 2, ...}
    """
    logger.info("Fetching log_type IDs from database…")

    out = {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM log_type")
        rows = cur.fetchall()
        for row in rows:
            out[row[1]] = row[0]

    logger.info(f"Loaded {len(out)} log types: {out}")
    return out


def get_action_type_id(conn, name: str) -> int:
    """
    Resolve an action_type ID.
    Auto-create the action_type if it does not exist.
    """
    with conn.cursor() as cur:
        logger.debug(f"Resolving action_type for: {name}")

        cur.execute("SELECT id FROM action_type WHERE name = %s", (name,))
        r = cur.fetchone()
        if r:
            return r[0]

        logger.info(f"action_type '{name}' not found. Creating new entry…")
        cur.execute(
            "INSERT INTO action_type (name) VALUES (%s) RETURNING id",
            (name,)
        )
        new_id = cur.fetchone()[0]
        logger.info(f"Created new action_type '{name}' with id {new_id}")
        conn.commit()
        return new_id


# ------------------------------------------------------------
# Main ingestion entrypoint
# ------------------------------------------------------------
def insert_logs(conn, parsed: Dict[str, List[Dict[str, Any]]]):
    """
    Insert parsed logs into PostgreSQL.

    parsed = {
        "access": [...],
        "dataxceiver": [...],
        "namesystem": [...]
    }
    """

    logger.info("Beginning log ingestion process…")

    log_type_ids = get_log_type_ids(conn)

    # Flatten everything
    all_rows = []
    for log_type_name, rows in parsed.items():
        logger.info(f"Preparing {len(rows)} rows for type '{log_type_name}'…")

        lt_id = log_type_ids[log_type_name]
        for row in rows:
            all_rows.append((log_type_name, lt_id, row))
        break

    logger.info(f"Total combined rows prepared for insertion: {len(all_rows)}")

    # Insert all logs
    insert_log_entries(conn, all_rows)

    logger.info("Ingestion process completed successfully.")


# ------------------------------------------------------------
# Insert log_entry + detail rows
# ------------------------------------------------------------
def insert_log_entries(conn, rows):
    """
    Insert log entries in batches.
    """
    logger.info("Starting batched insertion into log_entry…")

    entry_batch = []
    detail_batch = []

    with conn.cursor() as cur:
        for idx, (log_type_name, log_type_id, row) in enumerate(rows, 1):

            # --- Resolve action type
            action = row.get("action")
            action_id = get_action_type_id(conn, action) if action else None

            # --- Prepare the base log_entry row
            entry_batch.append((
                log_type_id,
                action_id,
                row.get("log_timestamp"),
                row.get("source_ip"),
                row.get("dest_ip"),
                row.get("block_id"),
                row.get("size_bytes"),
                row.get("file_name"),
                row.get("line_number"),
                row.get("raw_message"),
            ))

            # --- Access log extra detail
            detail = row.get("detail")
            if detail:
                detail_batch.append((idx, detail))

            # --- BATCH FLUSH
            if len(entry_batch) >= BATCH_SIZE:
                logger.info(f"Batch size reached ({BATCH_SIZE}). Flushing batch…")
                flush_entry_batch(conn, cur, entry_batch, detail_batch)
                entry_batch.clear()
                detail_batch.clear()

        # Final flush
        if entry_batch:
            logger.info(f"Final batch flush: {len(entry_batch)} rows remaining.")
            flush_entry_batch(conn, cur, entry_batch, detail_batch)

    logger.info("All batches inserted.")


# ------------------------------------------------------------
# Actual batch insertion into DB
# ------------------------------------------------------------
def flush_entry_batch(conn, cur, entry_batch, detail_batch):
    """
    Insert a batch of log_entry rows, then their related log_access_detail rows.
    """
    logger.info(f"Inserting {len(entry_batch)} rows into log_entry…")

    try:
        cur.executemany(
            """
            INSERT INTO log_entry (
                log_type_id, action_type_id, log_timestamp,
                source_ip, dest_ip,
                block_id, size_bytes,
                file_name, line_number,
                raw_message
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            entry_batch
        )

        entry_ids = [row[0] for row in cur.fetchall()]
        logger.info(f"Inserted {len(entry_ids)} log_entry rows successfully.")

    except Exception as exc:
        logger.error("Failed to insert log_entry batch.", exc_info=True)
        raise exc

    # --------------------------------------------------------
    # Insert detail rows (ACCESS logs only)
    # --------------------------------------------------------
    if detail_batch:
        logger.info(f"Preparing to insert {len(detail_batch)} access detail rows…")

        detail_rows = []
        for entry_id, (_, detail) in zip(entry_ids, detail_batch):
            detail_rows.append((
                entry_id,
                detail.get("remote_name"),
                detail.get("auth_user"),
                detail.get("http_method"),
                detail.get("resource"),
                detail.get("http_status"),
                detail.get("referrer"),
                detail.get("user_agent"),
            ))

        try:
            cur.executemany(
                """
                INSERT INTO log_access_detail (
                    log_entry_id,
                    remote_name, auth_user,
                    http_method, resource, http_status,
                    referrer, user_agent
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                detail_rows
            )
            logger.info(f"Inserted {len(detail_rows)} access_detail rows.")

        except Exception:
            logger.error("Failed to insert access detail batch.", exc_info=True)
            raise

    # --------------------------------------------------------
    # Commit after each batch
    # --------------------------------------------------------
    logger.info("Committing batch to database…")
    conn.commit()
    logger.info("Batch committed successfully.")
