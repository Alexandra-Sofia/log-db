#!/usr/bin/env python3
"""
Parse raw log files into CSVs suitable for COPY into PostgreSQL.

Outputs (in ./parsed by default):
  - log_type.csv
  - action_type.csv
  - log_entry.csv
  - log_access_detail.csv
"""

import os
import re
import csv
import json
import uuid
from itertools import count
from datetime import datetime, timezone

from util import LogType


# ============================================================
# Filenames
# ============================================================

LOG_TYPE_FILENAME = "log_type.csv"
ACTION_TYPE_FILENAME = "action_type.csv"
LOG_ENTRY_FILENAME = "log_entry.csv"
ACCESS_DETAIL_FILENAME = "log_access_detail.csv"

# Fields for log_entry.csv
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


# ============================================================
# Regex patterns
# ============================================================

ACCESS_REGEX = re.compile(
    r'(?P<ip>\S+) (?P<remote_name>\S+) (?P<auth_user>\S+) '
    r'\[(?P<timestamp>.+?)\] '
    r'"(?P<method>\S+) (?P<resource>\S+) \S+" '
    r'(?P<status>\d{3}) '
    r'(?P<size>\S+) '
    r'"(?P<referrer>.*?)" "(?P<agent>.*?)"'
)

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


# ============================================================
# Helpers and ID generators
# ============================================================

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{ts} | {msg}", flush=True)


def ts_apache(s: str) -> datetime:
    return datetime.strptime(s, "%d/%b/%Y:%H:%M:%S %z")


def ts_hdfs_compact(date: str, time: str) -> datetime:
    return datetime.strptime(date + time, "%y%m%d%H%M%S")


# In-memory mapping from LogType to integer ID
LOG_TYPE_IDS: dict[LogType, int] = {}

# In-memory mapping from action name -> integer ID
ACTION_TYPE_IDS: dict[str, int] = {}
_action_type_counter = count(1)


def init_log_type_ids(outdir: str) -> None:
    """
    Assign small integer IDs to each LogType and write log_type.csv.
    Full snapshot every run.
    """
    global LOG_TYPE_IDS

    log_type_path = os.path.join(outdir, LOG_TYPE_FILENAME)
    log(f"Writing log_type CSV to: {log_type_path}")

    with open(log_type_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()

        for idx, lt in enumerate(LogType.list(), start=1):
            LOG_TYPE_IDS[lt] = idx
            writer.writerow({"id": idx, "name": lt.value})


def get_action_type_id(action: str) -> int:
    """
    Get or assign a small integer ID for a given action_type name.
    """
    if action not in ACTION_TYPE_IDS:
        ACTION_TYPE_IDS[action] = next(_action_type_counter)
    return ACTION_TYPE_IDS[action]


def write_action_type_csv(outdir: str) -> None:
    """
    Write full snapshot of action types seen in this run into action_type.csv.
    """
    action_type_path = os.path.join(outdir, ACTION_TYPE_FILENAME)
    log(f"Writing action_type CSV to: {action_type_path}")

    with open(action_type_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()

        for name, id_ in sorted(ACTION_TYPE_IDS.items(), key=lambda kv: kv[1]):
            writer.writerow({"id": id_, "name": name})


def write_entry(writer_entry,
                log_type: LogType,
                action: str,
                timestamp: datetime,
                source_ip: str,
                dest_ip: str,
                block_id,
                size_bytes) -> str:
    """
    Unified writer for log_entry.csv using numeric IDs for log_type and action_type.
    """
    entry_id = str(uuid.uuid4())
    log_type_id = LOG_TYPE_IDS[log_type]
    action_type_id = get_action_type_id(action)

    writer_entry.writerow({
        "id": entry_id,
        "log_type_id": log_type_id,
        "action_type_id": action_type_id,
        "log_timestamp": timestamp.isoformat(),
        "source_ip": source_ip,
        "dest_ip": dest_ip,
        "block_id": block_id if block_id != "" else "",
        "size_bytes": size_bytes if size_bytes != "" else "",
    })
    return entry_id


# ============================================================
# ACCESS parser
# ============================================================

def parse_access(path: str, writer_entry, outdir: str) -> None:
    log(f"Parsing ACCESS log: {path}")
    total = 0
    matched = 0

    detail_csv_path = os.path.join(outdir, ACCESS_DETAIL_FILENAME)
    log(f"Writing ACCESS detail CSV to: {detail_csv_path}")

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

    with open(detail_csv_path, "w", newline="", encoding="utf-8") as det_csv:
        writer_detail = csv.DictWriter(det_csv, fieldnames=ACCESS_DETAIL_FIELDS)
        writer_detail.writeheader()

        with open(path, encoding="utf-8") as f:
            for raw in f:
                total += 1
                m = ACCESS_REGEX.match(raw.rstrip("\n"))
                if not m:
                    continue

                matched += 1
                g = m.groupdict()

                size = None if g["size"] == "-" else int(g["size"])
                ts = ts_apache(g["timestamp"])

                entry_id = write_entry(
                    writer_entry,
                    LogType.ACCESS,
                    g["method"],
                    ts,
                    g["ip"],
                    "",
                    "",
                    size if size is not None else "",
                )

                writer_detail.writerow({
                    "log_entry_id": entry_id,
                    "remote_name": g["remote_name"],
                    "auth_user": g["auth_user"],
                    "http_method": g["method"],
                    "resource": g["resource"],
                    "http_status": int(g["status"]),
                    "referrer": None if g["referrer"] == "-" else g["referrer"],
                    "user_agent": g["agent"],
                })

    log(f"ACCESS parsing done: matched {matched}/{total}")


# ============================================================
# DataXceiver parser
# ============================================================

def parse_dataxceiver(path: str, writer_entry) -> None:
    log(f"Parsing HDFS_DataXceiver log: {path}")
    total = 0
    matched = 0

    with open(path, encoding="utf-8") as f:
        for raw in f:
            total += 1
            m = DATAX_REGEX.match(raw.rstrip("\n"))
            if not m:
                continue

            matched += 1
            g = m.groupdict()
            ts = ts_hdfs_compact(g["date"], g["time"])

            if g.get("op_receiving"):
                write_entry(
                    writer_entry,
                    LogType.HDFS_DATAXCEIVER,
                    "receiving",
                    ts,
                    g["src_receiving"],
                    g["dst_receiving"],
                    int(g["blk_receiving"].replace("blk_", "")),
                    "",
                )
                continue

            if g.get("op_received"):
                size = g.get("size_received")
                write_entry(
                    writer_entry,
                    LogType.HDFS_DATAXCEIVER,
                    "received",
                    ts,
                    g["src_received"],
                    g["dst_received"],
                    int(g["blk_received"].replace("blk_", "")),
                    int(size) if size else "",
                )
                continue

            if g.get("op_served"):
                write_entry(
                    writer_entry,
                    LogType.HDFS_DATAXCEIVER,
                    "served",
                    ts,
                    g["src_served"],
                    g["dst_served"],
                    int(g["blk_served"].replace("blk_", "")),
                    "",
                )
                continue

    log(f"HDFS_DataXceiver parsing done: matched {matched}/{total}")


# ============================================================
# Namesystem parser
# ============================================================

def parse_namesystem(path: str, writer_entry) -> None:
    log(f"Parsing HDFS_FSNamesystem log: {path}")
    total = 0
    matched = 0

    with open(path, encoding="utf-8") as f:
        for raw in f:
            total += 1

            m_upd = NAMESYS_UPDATE_REGEX.match(raw.rstrip("\n"))
            if m_upd:
                matched += 1
                g = m_upd.groupdict()
                ts = ts_hdfs_compact(g["date"], g["time"])

                write_entry(
                    writer_entry,
                    LogType.HDFS_NAMESYSTEM,
                    "update",
                    ts,
                    "",
                    g["ip"],
                    int(g["block"]),
                    int(g["size"]) if g.get("size") else "",
                )
                continue

            m_rep = NAMESYS_ASK_REPLICATE_REGEX.match(raw.rstrip("\n"))
            if m_rep:
                g = m_rep.groupdict()
                ts = ts_hdfs_compact(g["date"], g["time"])
                src_ip = g["src_ip"]
                block_id = int(g["block"])

                for tok in g["dest_list"].split():
                    if ":" not in tok:
                        continue

                    matched += 1
                    dest_ip = tok.split(":", 1)[0]

                    write_entry(
                        writer_entry,
                        LogType.HDFS_NAMESYSTEM,
                        "replicate",
                        ts,
                        src_ip,
                        dest_ip,
                        block_id,
                        "",
                    )

    log(f"HDFS_FSNamesystem parsing done: matched {matched}/{total}")


# ============================================================
# Driver
# ============================================================

def main(logdir: str = "/input-logfiles", outdir: str = "./parsed") -> None:
    os.makedirs(outdir, exist_ok=True)

    # 1. Initialize log_type IDs and write log_type.csv
    init_log_type_ids(outdir)

    # 2. Open log_entry.csv for all log rows
    entry_path = os.path.join(outdir, LOG_ENTRY_FILENAME)
    log(f"Writing log_entry CSV to: {entry_path}")

    with open(entry_path, "w", newline="", encoding="utf-8") as entry_csv:
        writer_entry = csv.DictWriter(entry_csv, fieldnames=ENTRY_FIELDS)
        writer_entry.writeheader()

        parse_access(
            os.path.join(logdir, LogType.ACCESS.filename),
            writer_entry,
            outdir,
        )

        parse_dataxceiver(
            os.path.join(logdir, LogType.HDFS_DATAXCEIVER.filename),
            writer_entry,
        )

        parse_namesystem(
            os.path.join(logdir, LogType.HDFS_NAMESYSTEM.filename),
            writer_entry,
        )

    # 3. Write action_type.csv snapshot
    write_action_type_csv(outdir)

    log("All CSVs created successfully.")


if __name__ == "__main__":
    main()
