import uuid
from typing import Dict

from util import LogType
from config import ACTION_TYPE_NAMESPACE


def load_log_type_ids() -> Dict[LogType, int]:
    """
    Create a deterministic mapping of LogType enum values to numeric IDs.

    The IDs are stable and assigned in enumeration order, starting from 1.

    :return: A mapping from LogType to integer ID.
    """
    log_type_ids: Dict[LogType, int] = {}

    for idx, lt in enumerate(LogType.list(), start=1):
        log_type_ids[lt] = idx

    return log_type_ids


def deterministic_action_type_id(action: str) -> str:
    """
    Compute a deterministic UUID for an action type.

    The UUID is derived using a fixed namespace and the action name,
    ensuring consistency across processes and repeated runs.

    :param action: Action name (e.g., "GET", "replicate").
    :return: Deterministic UUID string.
    """
    return str(uuid.uuid5(ACTION_TYPE_NAMESPACE, action))
