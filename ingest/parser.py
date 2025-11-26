#!/usr/bin/env python3
import re
import os
from datetime import datetime
from typing import Callable, Dict, List, Optional, Any
import csv

from util import tiny_logger


def write_rows_to_csv(path: str, rows: list) -> str | None:
    if not rows:
        return None

    os.makedirs(".parsed", exist_ok=True)

    out_name = os.path.basename(path) + ".csv"
    out_path = os.path.join(".parsed", out_name)

    fieldnames = list(rows[0].keys())

    with open(out_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return out_path


def ts_apache(s: str) -> datetime:
    return datetime.strptime(s, "%d/%b/%Y:%H:%M:%S %z")


def ts_hdfs_compact(date: str, time: str) -> datetime:
    return datetime.strptime(date + time, "%y%m%d%H%M%S")


def make_row(
    *,
    log_type_name: str,
    action_type_name: str,
    log_timestamp: datetime,
    source_ip: Optional[str],
    dest_ip: Optional[str],
    block_id: Optional[int],
    size_bytes: Optional[int],
    detail: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    return {
        "log_type_name": log_type_name,
        "action_type_name": action_type_name,
        "log_timestamp": log_timestamp,
        "source_ip": source_ip,
        "dest_ip": dest_ip,
        "block_id": block_id,
        "size_bytes": size_bytes,
        "detail": detail
    }


def parse_file(
    path: str,
    regex: re.Pattern,
    row_builder: Callable[[Dict[str, str], int], List[Dict[str, Any]]]
) -> List[Dict[str, Any]]:
    tiny_logger(f"[parse_file] Starting: {path}")
    rows = []
    total = 0
    matched = 0

    with open(path) as f:
        for line_no, raw in enumerate(f, 1):
            total += 1
            clean = raw.rstrip("\n")
            m = regex.match(clean)

            if not m:
                continue

            matched += 1
            g = m.groupdict()
            built_rows = row_builder(g, line_no)

            for row in built_rows:
                rows.append(
                    make_row(
                        log_type_name=row["log_type_name"],
                        action_type_name=row["action_type_name"],
                        log_timestamp=row["timestamp"],
                        source_ip=row["source_ip"],
                        dest_ip=row["dest_ip"],
                        block_id=row["block_id"],
                        size_bytes=row["size_bytes"],
                        detail=row["detail"]
                    )
                )

    tiny_logger(f"[parse_file] Finished {path}: matched {matched}/{total}")

    return rows


ACCESS_REGEX = re.compile(
    r'(?P<ip>\S+) (?P<remote_name>\S+) (?P<auth_user>\S+) '
    r'\[(?P<timestamp>.+?)\] '
    r'"(?P<method>\S+) (?P<resource>\S+) \S+" '
    r'(?P<status>\d{3}) '
    r'(?P<size>\S+) '
    r'"(?P<referrer>.*?)" "(?P<agent>.*?)"'
)


def build_access(g: Dict[str, str], _: int) -> List[Dict[str, Any]]:
    size = None if g["size"] == "-" else int(g["size"])
    timestamp = ts_apache(g["timestamp"])

    return [{
        "log_type_name": "ACCESS",
        "action_type_name": g["method"],
        "timestamp": timestamp,
        "source_ip": g["ip"],
        "dest_ip": None,
        "block_id": None,
        "size_bytes": size,
        "detail": {
            "remote_name": g["remote_name"],
            "auth_user": g["auth_user"],
            "http_method": g["method"],
            "resource": g["resource"],
            "http_status": int(g["status"]),
            "referrer": None if g["referrer"] == "-" else g["referrer"],
            "user_agent": g["agent"]
        }
    }]


DATAX_REGEX = re.compile(
    r'''
    ^
    (?P<date>\d{6})\s+
    (?P<time>\d{6})\s+
    (?P<tid>\d+)\s+
    INFO\s+dfs\.DataNode\$DataXceiver:\s+

    (?:
        (?P<op_receiving>Receiving)\s+block\s+
        (?P<blk_receiving>blk_[0-9\-]+)
        \s+src:\s+/(?P<src_receiving>[0-9.]+):\d+
        \s+dest:\s+/(?P<dst_receiving>[0-9.]+):\d+
        |

        (?P<op_received>Received)\s+block\s+
        (?P<blk_received>blk_[0-9\-]+)
        .*?src:\s+/(?P<src_received>[0-9.]+):\d+
        \s+dest:\s+/(?P<dst_received>[0-9.]+):\d+
        (?:.*?size\s+(?P<size_received>\d+))?
        |

        (?P<src_served>[0-9.]+):\d+\s+
        (?P<op_served>Served)\s+block\s+
        (?P<blk_served>blk_[0-9\-]+)
        \s+to\s+/(?P<dst_served>[0-9.]+)
    )
    $
    ''',
    re.VERBOSE
)


def build_datax(g: Dict[str, str], _: int) -> List[Dict[str, Any]]:
    timestamp = ts_hdfs_compact(g["date"], g["time"])

    if g.get("op_receiving"):
        return [{
            "log_type_name": "HDFS_DATAXCEIVER",
            "action_type_name": "receiving",
            "timestamp": timestamp,
            "source_ip": g["src_receiving"],
            "dest_ip": g["dst_receiving"],
            "block_id": int(g["blk_receiving"][4:]),
            "size_bytes": None,
            "detail": None
        }]

    if g.get("op_received"):
        size = int(g["size_received"]) if g.get("size_received") else None
        return [{
            "log_type_name": "HDFS_DATAXCEIVER",
            "action_type_name": "received",
            "timestamp": timestamp,
            "source_ip": g["src_received"],
            "dest_ip": g["dst_received"],
            "block_id": int(g["blk_received"][4:]),
            "size_bytes": size,
            "detail": None
        }]

    if g.get("op_served"):
        return [{
            "log_type_name": "HDFS_DATAXCEIVER",
            "action_type_name": "served",
            "timestamp": timestamp,
            "source_ip": g["src_served"],
            "dest_ip": g["dst_served"],
            "block_id": int(g["blk_served"][4:]),
            "size_bytes": None,
            "detail": None
        }]

    return []


NAMESYS_UPDATE_REGEX = re.compile(
    r'''
    ^
    (?P<date>\d{6})\s+
    (?P<time>\d{6})\s+
    (?P<tid>\d+)\s+
    INFO\s+dfs\.FSNamesystem:\s+BLOCK\*\s+
    NameSystem\.\w+:\s+
    blockMap\s+updated:\s+
    (?P<ip>[0-9.]+):\d+.*?
    blk_(?P<block>-?\d+)
    (?:\s+size\s+(?P<size>\d+))?
    $
    ''',
    re.VERBOSE
)

NAMESYS_ASK_REPLICATE_REGEX = re.compile(
    r'''
    ^
    (?P<date>\d{6})\s+
    (?P<time>\d{6})\s+
    (?P<tid>\d+)\s+
    INFO\s+dfs\.FSNamesystem:\s+BLOCK\*\s+
    ask\s+(?P<src_ip>[0-9.]+):\d+
    \s+to\s+replicate\s+
    blk_(?P<block>-?\d+)
    \s+to\s+datanode\(s\)\s+
    (?P<dest_list>(?:[0-9.]+:\d+\s*)+)
    $
    ''',
    re.VERBOSE
)


def build_namesystem_update(g: Dict[str, str], _: int) -> List[Dict[str, Any]]:
    timestamp = ts_hdfs_compact(g["date"], g["time"])
    size = int(g["size"]) if g.get("size") else None

    return [{
        "log_type_name": "HDFS_NAMESYSTEM",
        "action_type_name": "update",
        "timestamp": timestamp,
        "source_ip": None,
        "dest_ip": g.get("ip"),
        "block_id": int(g["block"]),
        "size_bytes": size,
        "detail": None
    }]


def build_namesystem_replicate(g: Dict[str, str], _: int) -> List[Dict[str, Any]]:
    timestamp = ts_hdfs_compact(g["date"], g["time"])
    block_id = int(g["block"])
    src_ip = g["src_ip"]

    rows = []
    for token in g["dest_list"].split():
        ip = token.split(":")[0]
        rows.append({
            "log_type_name": "HDFS_NAMESYSTEM",
            "action_type_name": "replicate",
            "timestamp": timestamp,
            "source_ip": src_ip,
            "dest_ip": ip,
            "block_id": block_id,
            "size_bytes": None,
            "detail": None
        })

    return rows


def parse_namesystem(path: str) -> List[Dict[str, Any]]:
    tiny_logger(f"[parse_namesystem] Starting: {path}")
    rows = []
    total = 0
    matched = 0

    with open(path) as f:
        for line_no, raw in enumerate(f, 1):
            total += 1
            clean = raw.rstrip("\n")

            m_upd = NAMESYS_UPDATE_REGEX.match(clean)
            if m_upd:
                for r in build_namesystem_update(m_upd.groupdict(), line_no):
                    rows.append(
                        make_row(
                            log_type_name=r["log_type_name"],
                            action_type_name=r["action_type_name"],
                            log_timestamp=r["timestamp"],
                            source_ip=r["source_ip"],
                            dest_ip=r["dest_ip"],
                            block_id=r["block_id"],
                            size_bytes=r["size_bytes"],
                            detail=r["detail"]
                        )
                    )
                matched += 1
                continue

            m_rep = NAMESYS_ASK_REPLICATE_REGEX.match(clean)
            if m_rep:
                built = build_namesystem_replicate(m_rep.groupdict(), line_no)
                for r in built:
                    rows.append(
                        make_row(
                            log_type_name=r["log_type_name"],
                            action_type_name=r["action_type_name"],
                            log_timestamp=r["timestamp"],
                            source_ip=r["source_ip"],
                            dest_ip=r["dest_ip"],
                            block_id=r["block_id"],
                            size_bytes=r["size_bytes"],
                            detail=r["detail"]
                        )
                    )
                matched += 1

    tiny_logger(f"[parse_namesystem] Finished {path}: matched {matched}/{total}")

    return rows


def parse_access(path: str) -> List[Dict[str, Any]]:
    return parse_file(path, ACCESS_REGEX, build_access)


def parse_dataxceiver(path: str) -> List[Dict[str, Any]]:
    return parse_file(path, DATAX_REGEX, build_datax)

