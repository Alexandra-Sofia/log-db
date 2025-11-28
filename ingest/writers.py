import uuid
from typing import Any, Dict, Optional

from util import LogType


def write_entry(
    writer_entry,
    log_type: LogType,
    action: str,
    timestamp,
    source_ip: Optional[str],
    dest_ip: Optional[str],
    block_id: Any,
    size_bytes: Any,
    detail: Dict[str, Any],
    log_type_ids: Dict[LogType, int],
) -> str:
    """
    Write a single log_entry row to the CSV output.

    A new UUID is generated for each entry and returned.
    Empty block_id or size_bytes values are normalized to empty strings.

    :param writer_entry: CSV DictWriter for log_entry rows.
    :param log_type: LogType for this entry.
    :param action: Deterministic UUID string for the action type.
    :param timestamp: Datetime object to serialize.
    :param source_ip: Source IP address (or None).
    :param dest_ip: Destination IP address (or None).
    :param block_id: Block identifier or empty string.
    :param size_bytes: Block size in bytes or empty string.
    :param detail: Reserved for future detail data (unused).
    :param log_type_ids: Mapping from LogType to integer ID.

    :return: Generated entry UUID as a string.
    """
    entry_id = str(uuid.uuid4())

    writer_entry.writerow({
        "id": entry_id,
        "log_type_id": log_type_ids[log_type],
        "action_type_id": action,
        "log_timestamp": timestamp.isoformat(),
        "source_ip": source_ip,
        "dest_ip": dest_ip,
        "block_id": block_id if block_id != "" else "",
        "size_bytes": size_bytes if size_bytes != "" else "",
    })

    return entry_id
