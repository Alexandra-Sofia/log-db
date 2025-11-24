import psycopg
from typing import Dict, List, Any
from logger import setup_logger

logger = setup_logger("log_ingest")

BATCH_SIZE = 5000


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def get_log_type_ids(conn) -> Dict[str, int]:
    """Return {'ACCESS': 1, 'HDFS_DATAXCEIVER': 2, ...}."""
    out = {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM log_type")
        for row in cur.fetchall():
            out[row[1]] = row[0]
    return out


def get_action_type_id(conn, name: str) -> int:
    """Auto-create action types if missing."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM action_type WHERE name = %s", (name,))
        r = cur.fetchone()
        if r:
            return r[0]

        # Insert new action type
        cur.execute("INSERT INTO action_type (name) VALUES (%s) RETURNING id", (name,))
        new_id = cur.fetchone()[0]
        logger.info(f"Created new action_type: {name} -> id {new_id}")
        return new_id


# ------------------------------------------------------------
# Main ingestion function
# ------------------------------------------------------------
def insert_logs(conn, parsed: Dict[str, List[Dict[str, Any]]]):
    """
    Insert parsed logs into PostgreSQL database.

    parsed = {
        "access": [...],
        "dataxceiver": [...],
        "namesystem": [...]
    }
    """

    log_type_ids = get_log_type_ids(conn)

    # Flatten all rows into one big list
    all_rows = []
    for log_type_name, rows in parsed.items():
        lt_id = log_type_ids[log_type_name]
        for row in rows:
            all_rows.append((log_type_name, lt_id, row))

    logger.info(f"Total rows prepared for insertion: {len(all_rows)}")

    # Insert log_entry rows in batches
    insert_log_entries(conn, all_rows)

    logger.info("Finished inserting all logs.")


# ------------------------------------------------------------
# Insert log_entry and log_access_detail
# ------------------------------------------------------------
def insert_log_entries(conn, rows):
    entry_batch = []
    detail_batch = []

    with conn.cursor() as cur:

        for idx, (log_type_name, log_type_id, row) in enumerate(rows, 1):

            action = row.get("action")
            action_id = get_action_type_id(conn, action) if action else None

            entry_batch.append((
                log_type_id,
                action_id,
                row["timestamp"],
                row.get("source_ip"),
                row.get("dest_ip"),
                row.get("block_id"),
                row.get("size_bytes"),
                row.get("file_name"),
                row.get("line_number"),
                row.get("raw_message")
            ))

            # Access entries have extra detail dict
            detail = row.get("detail")
            if detail:
                detail_batch.append((idx, detail))

            # Batch flush
            if len(entry_batch) >= BATCH_SIZE:
                flush_entry_batch(conn, cur, entry_batch, detail_batch)
                entry_batch.clear()
                detail_batch.clear()

        # Final flush
        if entry_batch:
            flush_entry_batch(conn, cur, entry_batch, detail_batch)


def flush_entry_batch(conn, cur, entry_batch, detail_batch):
    logger.info(f"Inserting batch of {len(entry_batch)} log_entry rows…")

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

    # Insert access details
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
            detail.get("user_agent")
        ))

    if detail_rows:
        logger.info(f"Inserting {len(detail_rows)} access detail rows…")

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

    conn.commit()
