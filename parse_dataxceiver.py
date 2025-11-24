#!/usr/bin/env python3
import re
import os
from datetime import datetime
from typing import Callable, Dict, List, Optional, Any

from logger import setup_logger
logger = setup_logger("log_parser")


# ----------------------------
# DATA XCEIVER MULTI-REGEX SET
# ----------------------------

DATAX_RECEIVING = re.compile(
    r'^(?P<date>\d{6})\s+'
    r'(?P<time>\d{6})\s+\d+\s+INFO\s+dfs\.DataNode\$DataXceiver:\s+'
    r'Receiving block blk_(?P<block>-?\d+)\s+'
    r'src:\s*/(?P<src_ip>[\d\.]+):\d+\s+'
    r'dest:\s*/(?P<dest_ip>[\d\.]+):\d+'
)

DATAX_RECEIVED = re.compile(
    r'^(?P<date>\d{6})\s+'
    r'(?P<time>\d{6})\s+\d+\s+INFO\s+dfs\.DataNode\$DataXceiver:\s+'
    r'Received block blk_(?P<block>-?\d+)'
    r'(?:\s+src:\s*/(?P<src_ip>[\d\.]+):\d+)?'
    r'(?:\s+dest:\s*/(?P<dest_ip>[\d\.]+):\d+)?'
    r'(?:\s+of\s+size\s+(?P<size>\d+))?'
)

DATAX_SERVED = re.compile(
    r'^(?P<date>\d{6})\s+'
    r'(?P<time>\d{6})\s+\d+\s+INFO\s+dfs\.DataNode\$DataXceiver:\s+'
    r'(?P<prefix_ip>[\d\.]+):\d+\s+Served block blk_(?P<block>-?\d+)'
    r'\s+to\s+/(?P<dest_ip>[\d\.]+)'
)

# Ordered list of patterns
DATAX_PATTERNS = [
    ("receiving", DATAX_RECEIVING),
    ("received",  DATAX_RECEIVED),
    ("served",    DATAX_SERVED),
]

def build_datax(g: Dict[str, str], op: str, line_no: int) -> List[Dict[str, Any]]:
    timestamp = ts_hdfs_compact(g["date"], g["time"])

    src = g.get("src_ip")
    dest = g.get("dest_ip")
    prefix = g.get("prefix_ip")
    size = g.get("size")

    if prefix and not src:
        src = prefix

    entry = {
        "log_type_name": "HDFS_DATAXCEIVER",
        "action_type_name": op,
        "timestamp": timestamp,
        "source_ip": src,
        "dest_ip": dest,
        "block_id": int(g["block"]),
        "size_bytes": int(size) if size else None,
        "detail": None
    }

    return [entry]
