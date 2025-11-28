import uuid
from util import LogType
from config import ACTION_TYPE_NAMESPACE

def load_log_type_ids():
    ids = {}
    for idx, lt in enumerate(LogType.list(), start=1):
        ids[lt] = idx
    return ids

def deterministic_action_type_id(action: str) -> str:
    return str(uuid.uuid5(ACTION_TYPE_NAMESPACE, action))
