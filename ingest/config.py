import os
import uuid
from util import LogType

LOG_TYPE_FILENAME = "log_type.csv"
ACTION_TYPE_FILENAME = "action_type.csv"
LOG_ENTRY_FILENAME = "log_entry.csv"
ACCESS_DETAIL_FILENAME = "log_access_detail.csv"

TMP_DIRNAME = "tmp"

ENTRY_FIELDS = [
    "id",
    "log_type_id",
    "action_type_id",
    "log_timestamp",
    "source_ip",
    "dest_ip",
    "block_id",
    "size_bytes",
]

ACCESS_DETAIL_FIELDS = [
    "log_entry_id",
    "remote_name",
    "auth_user",
    "http_method",
    "resource",
    "http_status",
    "referrer",
    "user_agent",
]

ACTION_TYPE_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")
