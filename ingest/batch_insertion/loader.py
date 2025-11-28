from typing import Dict, List, Any, Tuple
import psycopg2
from psycopg2.extras import execute_values

from util import tiny_logger, LogType

BATCH_SIZE = 500000

def get_log_type_ids(conn) -> Dict[str, int]:
    """
    Load all ``log_type`` rows.

    Returns
    -------
    dict
        A mapping of ``NAME_UPPER -> id``.
    """
    tiny_logger("Fetching log_type IDs from database...")
    mapping: Dict[str, int] = {}

    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM log_type;")
        for id_, name in cur.fetchall():
            mapping[name.upper()] = id_

    tiny_logger(f"Loaded {len(mapping)} log types: {mapping}")
    return mapping


def get_action_type_id(conn, name: str | None) -> int | None:
    """
    Resolve or create an ``action_type``.

    Parameters
    ----------
    name : str or None
        The action type name.

    Returns
    -------
    int or None
        The id of the action type, or ``None`` if name is missing.
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
        tiny_logger(f"Created new action_type: {name} -> id {new_id}")
        return new_id


def load(conn, parsed: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Insert all parsed logs into the database.

    Parameters
    ----------
    conn : psycopg2 connection
        Active database connection.
    parsed : dict
        Mapping of log type name to parsed rows. Keys expected:
        ``ACCESS``, ``HDFS_DATAXCEIVER``, ``HDFS_NAMESYSTEM``.
    """
    tiny_logger("Beginning log ingestion process...")
    tiny_logger(f"Parsed log types present: {list(parsed.keys())}")

    log_type_ids = get_log_type_ids(conn)

    with conn:
        with conn.cursor() as cur:
            total_rows = 0

            for key, rows in parsed.items():
                lt_name = key.upper()
                if lt_name not in log_type_ids:
                    tiny_logger(f"Skipping unknown log_type {lt_name}")
                    continue

                lt_id = log_type_ids[lt_name]
                tiny_logger(f"Preparing {len(rows)} rows for type '{lt_name}'")

                inserted = _insert_for_log_type(conn, cur, lt_id, rows)
                total_rows += inserted

                tiny_logger(f"Inserted {inserted} rows for '{lt_name}'")

            tiny_logger(f"FINAL: total inserted into log_entry = {total_rows}")


def _insert_for_log_type(conn, cur, log_type_id: int, rows: List[Dict[str, Any]]) -> int:
    """
    Insert all rows of a specific log type in batches.

    Parameters
    ----------
    log_type_id : int
        ID of the log type.
    rows : list of dict
        Parsed rows for this log type.

    Returns
    -------
    int
        Number of inserted ``log_entry`` rows.
    """
    entry_batch: List[Tuple[Any, ...]] = []
    detail_staging: List[Tuple[int, Dict[str, Any]]] = []
    inserted_count = 0

    for row in rows:
        action_name = row.get("action_type_name")
        action_id = get_action_type_id(conn, action_name)

        idx = len(entry_batch)
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

    if entry_batch:
        inserted = _flush_entry_batch(cur, entry_batch, detail_staging)
        inserted_count += inserted

    return inserted_count


def _flush_entry_batch(cur, entry_batch, detail_staging) -> int:
    """
    Flush a batch of ``log_entry`` rows and associated ``log_access_detail`` rows.

    Parameters
    ----------
    cur : cursor
        Active psycopg2 cursor.
    entry_batch : list
        Batched ``log_entry`` rows.
    detail_staging : list
        Staged ACCESS detail rows, storing index -> detail mapping.

    Returns
    -------
    int
        Number of inserted ``log_entry`` rows.
    """
    if not entry_batch:
        return 0

    tiny_logger(f"Flushing batch of {len(entry_batch)} rows")

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
        entry_ids = [row[0] for row in returned]
    except Exception as exc:
        tiny_logger("Failed to insert log_entry batch")
        raise

    tiny_logger(f"Inserted {len(entry_ids)} log_entry rows")

    if detail_staging:
        tiny_logger(f"Inserting {len(detail_staging)} ACCESS detail rows")

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
        tiny_logger("ACCESS detail insertion complete")

    return len(entry_ids)
