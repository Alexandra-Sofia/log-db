#!/usr/bin/env python3
"""
Parse raw log files into a single CSV suitable for COPY into PostgreSQL.

The output file is: ./parsed/log_entry.csv
"""

import os
import re
import csv
import json
from datetime import datetime, timezone
from typing import Dict, Any, Iterable, Optional, List

# ----------------------------
# Logging helper
# ----------------------------

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{ts} | {msg}", flush=True)


# ----------------------------
# Timestamp helpers
# ----------------------------

def ts_apache(s: str) -> datetime:
    return datetime.strptime(s, "%d/%b/%Y:%H:%M:%S %z")


def ts_hdfs_compact(date: str, time: str) -> datetime:
    return datetime.strptime(date + time, "%y%m%d%H%M%S")


# ----------------------------
# Apache access.log parsing
# ----------------------------

ACCESS_REGEX = re.compile(
    r'(?P<ip>\S+) (?P<remote_name>\S+) (?P<auth_user>\S+) '
    r'\[(?P<timestamp>.+?)\] '
    r'"(?P<method>\S+) (?P<resource>\S+) \S+" '
    r'(?P<status>\d{3}) '
    r'(?P<size>\S+) '
    r'"(?P<referrer>.*?)" "(?P<agent>.*?)"'
)


def parse_access(path: str) -> Iterable[Dict[str, Any]]:
    log(f"Parsing ACCESS log: {path}")
    total = 0
    matched = 0

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

            detail = {
                "remote_name": g["remote_name"],
                "auth_user": g["auth_user"],
                "http_method": g["method"],
                "resource": g["resource"],
                "http_status": int(g["status"]),
                "referrer": None if g["referrer"] == "-" else g["referrer"],
                "user_agent": g["agent"],
            }

            yield {
                "log_type_name": "ACCESS",
                "action_type_name": g["method"],
                "log_timestamp": ts.isoformat(),
                "source_ip": g["ip"],
                "dest_ip": "",
                "block_id": "",
                "size_bytes": size if size is not None else "",
                "detail": json.dumps(detail, ensure_ascii=False),
            }

    log(f"ACCESS parsing done: matched {matched}/{total}")


# ----------------------------
# HDFS DataXceiver parsing
# ----------------------------

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


def parse_dataxceiver(path: str) -> Iterable[Dict[str, Any]]:
    log(f"Parsing HDFS_DataXceiver log: {path}")
    total = 0
    matched = 0

    with open(path, encoding="utf-8") as f:
        for raw in f:
            total += 1
            line = raw.rstrip("\n")
            m = DATAX_REGEX.match(line)
            if not m:
                continue
            matched += 1
            g = m.groupdict()
            ts = ts_hdfs_compact(g["date"], g["time"])

            # RECEIVING
            if g.get("op_receiving"):
                block_id = int(g["blk_receiving"].replace("blk_", ""))
                yield {
                    "log_type_name": "HDFS_DATAXCEIVER",
                    "action_type_name": "receiving",
                    "log_timestamp": ts.isoformat(),
                    "source_ip": g["src_receiving"],
                    "dest_ip": g["dst_receiving"],
                    "block_id": block_id,
                    "size_bytes": "",
                    "detail": "{}",
                }
                continue

            # RECEIVED
            if g.get("op_received"):
                block_id = int(g["blk_received"].replace("blk_", ""))
                size = g.get("size_received")
                size_val = int(size) if size else ""
                yield {
                    "log_type_name": "HDFS_DATAXCEIVER",
                    "action_type_name": "received",
                    "log_timestamp": ts.isoformat(),
                    "source_ip": g["src_received"],
                    "dest_ip": g["dst_received"],
                    "block_id": block_id,
                    "size_bytes": size_val,
                    "detail": "{}",
                }
                continue

            # SERVED
            if g.get("op_served"):
                block_id = int(g["blk_served"].replace("blk_", ""))
                yield {
                    "log_type_name": "HDFS_DATAXCEIVER",
                    "action_type_name": "served",
                    "log_timestamp": ts.isoformat(),
                    "source_ip": g["src_served"],
                    "dest_ip": g["dst_served"],
                    "block_id": block_id,
                    "size_bytes": "",
                    "detail": "{}",
                }
                continue

    log(f"HDFS_DataXceiver parsing done: matched {matched}/{total}")


# ----------------------------
# Namesystem parsing
# ----------------------------

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


def parse_namesystem(path: str) -> Iterable[Dict[str, Any]]:
    log(f"Parsing HDFS_FSNamesystem log: {path}")
    total = 0
    matched = 0

    with open(path, encoding="utf-8") as f:
        for raw in f:
            total += 1
            line = raw.rstrip("\n")

            m_upd = NAMESYS_UPDATE_REGEX.match(line)
            if m_upd:
                matched += 1
                g = m_upd.groupdict()
                ts = ts_hdfs_compact(g["date"], g["time"])
                size_val = int(g["size"]) if g.get("size") else ""
                yield {
                    "log_type_name": "HDFS_NAMESYSTEM",
                    "action_type_name": "update",
                    "log_timestamp": ts.isoformat(),
                    "source_ip": "",
                    "dest_ip": g["ip"],
                    "block_id": int(g["block"]),
                    "size_bytes": size_val,
                    "detail": "{}",
                }
                continue

            m_rep = NAMESYS_ASK_REPLICATE_REGEX.match(line)
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
                    yield {
                        "log_type_name": "HDFS_NAMESYSTEM",
                        "action_type_name": "replicate",
                        "log_timestamp": ts.isoformat(),
                        "source_ip": src_ip,
                        "dest_ip": dest_ip,
                        "block_id": block_id,
                        "size_bytes": "",
                        "detail": "{}",
                    }

    log(f"HDFS_FSNamesystem parsing done: matched {matched}/{total}")


# ----------------------------
# Driver
# ----------------------------

def main(logdir: str = "/input-logfiles", outdir: str = "./parsed") -> None:
    os.makedirs(outdir, exist_ok=True)
    out_path = os.path.join(outdir, "log_entry.csv")

    log(f"Writing CSV to: {out_path}")

    fieldnames = [
        "log_type_name",
        "action_type_name",
        "log_timestamp",
        "source_ip",
        "dest_ip",
        "block_id",
        "size_bytes",
        "detail",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # ACCESS
        access_path = os.path.join(logdir, "access_log_full")
        for row in parse_access(access_path):
            writer.writerow(row)

        # DATAXCEIVER
        datax_path = os.path.join(logdir, "HDFS_DataXceiver.log")
        for row in parse_dataxceiver(datax_path):
            writer.writerow(row)

        # NAMESYSTEM
        namesys_path = os.path.join(logdir, "HDFS_FS_Namesystem.log")
        for row in parse_namesystem(namesys_path):
            writer.writerow(row)

    log(f"CSV created successfully: {out_path}")


if __name__ == "__main__":
    main()
