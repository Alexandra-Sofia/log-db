#!/usr/bin/env python3
"""
Parallel Log Parser:
- Three processes parse ACCESS, DATAXCEIVER, NAMESYSTEM logs in parallel.
- Each writes temporary CSVs into parsed/tmp/.
- Parent process merges them into final CSVs:
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
from datetime import datetime, timezone
from multiprocessing import Process

from util import LogType


# ============================================================
# Filenames
# ============================================================

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

# Deterministic namespace for action_type UUIDs
ACTION_TYPE_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")


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
# Logging & timestamps
# ============================================================

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{ts} | {msg}", flush=True)


def ts_apache(s: str) -> datetime:
    return datetime.strptime(s, "%d/%b/%Y:%H:%M:%S %z")


def ts_hdfs_compact(date: str, time: str) -> datetime:
    return datetime.strptime(date + time, "%y%m%d%H%M%S")


# ============================================================
# ID helpers
# ============================================================

def load_log_type_ids():
    """Static mapping from LogType enum."""
    ids = {}
    for idx, lt in enumerate(LogType.list(), start=1):
        ids[lt] = idx
    return ids


def deterministic_action_type_id(action: str) -> str:
    """
    Deterministic UUID per action name, same across all workers and runs.
    """
    return str(uuid.uuid5(ACTION_TYPE_NAMESPACE, action))


# ============================================================
# Core write_entry function (per process)
# ============================================================

def write_entry(writer_entry, log_type, action, timestamp,
                source_ip, dest_ip, block_id, size_bytes, detail,
                LOG_TYPE_IDS):
    entry_id = str(uuid.uuid4())
    writer_entry.writerow({
        "id": entry_id,
        "log_type_id": LOG_TYPE_IDS[log_type],
        "action_type_id": deterministic_action_type_id(action),
        "log_timestamp": timestamp.isoformat(),
        "source_ip": source_ip,
        "dest_ip": dest_ip,
        "block_id": block_id if block_id != "" else "",
        "size_bytes": size_bytes if size_bytes != "" else "",
    })
    return entry_id


# ============================================================
# Parsers (each running in its own process)
# ============================================================

def parse_access_worker(input_path, tmp_entry_path, tmp_detail_path):
    LOG_TYPE_IDS = load_log_type_ids()
    action_type_names = set()

    with open(tmp_entry_path, "w", newline="", encoding="utf-8") as entry_csv, \
         open(tmp_detail_path, "w", newline="", encoding="utf-8") as det_csv:

        w_entry = csv.DictWriter(entry_csv, fieldnames=ENTRY_FIELDS)
        w_entry.writeheader()
        w_detail = csv.DictWriter(det_csv, fieldnames=ACCESS_DETAIL_FIELDS)
        w_detail.writeheader()

        with open(input_path, encoding="utf-8") as f:
            for raw in f:
                m = ACCESS_REGEX.match(raw.rstrip("\n"))
                if not m:
                    continue
                g = m.groupdict()
                ts = ts_apache(g["timestamp"])

                action = g["method"]
                action_type_names.add(action)

                entry_id = write_entry(
                    w_entry,
                    LogType.ACCESS,
                    action,
                    ts,
                    g["ip"],
                    "",
                    "",
                    int(g["size"]) if g["size"] != "-" else "",
                    {},
                    LOG_TYPE_IDS,
                )

                w_detail.writerow({
                    "log_entry_id": entry_id,
                    "remote_name": g["remote_name"],
                    "auth_user": g["auth_user"],
                    "http_method": g["method"],
                    "resource": g["resource"],
                    "http_status": int(g["status"]),
                    "referrer": None if g["referrer"] == "-" else g["referrer"],
                    "user_agent": g["agent"],
                })

    # Save action types for parent (id, name via deterministic UUID)
    at_path = tmp_detail_path.replace("access_detail", "action_types")
    with open(at_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
        for name in sorted(action_type_names):
            writer.writerow({"id": deterministic_action_type_id(name), "name": name})


def parse_dataxceiver_worker(input_path, tmp_entry_path):
    LOG_TYPE_IDS = load_log_type_ids()
    action_type_names = set()

    with open(tmp_entry_path, "w", newline="", encoding="utf-8") as entry_csv:
        w_entry = csv.DictWriter(entry_csv, fieldnames=ENTRY_FIELDS)
        w_entry.writeheader()

        with open(input_path, encoding="utf-8") as f:
            for raw in f:
                m = DATAX_REGEX.match(raw.rstrip("\n"))
                if not m:
                    continue

                g = m.groupdict()
                ts = ts_hdfs_compact(g["date"], g["time"])

                if g.get("op_receiving"):
                    action = "receiving"
                    action_type_names.add(action)
                    write_entry(
                        w_entry,
                        LogType.HDFS_DATAXCEIVER,
                        action,
                        ts,
                        g["src_receiving"],
                        g["dst_receiving"],
                        int(g["blk_receiving"].replace("blk_", "")),
                        "",
                        {},
                        LOG_TYPE_IDS,
                    )

                elif g.get("op_received"):
                    action = "received"
                    action_type_names.add(action)
                    size = g["size_received"]
                    write_entry(
                        w_entry,
                        LogType.HDFS_DATAXCEIVER,
                        action,
                        ts,
                        g["src_received"],
                        g["dst_received"],
                        int(g["blk_received"].replace("blk_", "")),
                        int(size) if size else "",
                        {},
                        LOG_TYPE_IDS,
                    )

                elif g.get("op_served"):
                    action = "served"
                    action_type_names.add(action)
                    write_entry(
                        w_entry,
                        LogType.HDFS_DATAXCEIVER,
                        action,
                        ts,
                        g["src_served"],
                        g["dst_served"],
                        int(g["blk_served"].replace("blk_", "")),
                        "",
                        {},
                        LOG_TYPE_IDS,
                    )

    # Save action types
    at_path = tmp_entry_path.replace("log_entry", "action_types")
    with open(at_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
        for name in sorted(action_type_names):
            writer.writerow({"id": deterministic_action_type_id(name), "name": name})


def parse_namesystem_worker(input_path, tmp_entry_path):
    LOG_TYPE_IDS = load_log_type_ids()
    action_type_names = set()

    with open(tmp_entry_path, "w", newline="", encoding="utf-8") as entry_csv:
        w_entry = csv.DictWriter(entry_csv, fieldnames=ENTRY_FIELDS)
        w_entry.writeheader()

        with open(input_path, encoding="utf-8") as f:
            for raw in f:
                m_upd = NAMESYS_UPDATE_REGEX.match(raw.rstrip("\n"))
                if m_upd:
                    g = m_upd.groupdict()
                    ts = ts_hdfs_compact(g["date"], g["time"])
                    action = "update"
                    action_type_names.add(action)
                    write_entry(
                        w_entry,
                        LogType.HDFS_NAMESYSTEM,
                        action,
                        ts,
                        "",
                        g["ip"],
                        int(g["block"]),
                        int(g["size"]) if g.get("size") else "",
                        {},
                        LOG_TYPE_IDS,
                    )
                    continue

                m_rep = NAMESYS_ASK_REPLICATE_REGEX.match(raw.rstrip("\n"))
                if m_rep:
                    g = m_rep.groupdict()
                    ts = ts_hdfs_compact(g["date"], g["time"])
                    src_ip = g["src_ip"]
                    block_id = int(g["block"])
                    action = "replicate"
                    action_type_names.add(action)

                    for tok in g["dest_list"].split():
                        if ":" not in tok:
                            continue
                        dest_ip = tok.split(":", 1)[0]

                        write_entry(
                            w_entry,
                            LogType.HDFS_NAMESYSTEM,
                            action,
                            ts,
                            src_ip,
                            dest_ip,
                            block_id,
                            "",
                            {},
                            LOG_TYPE_IDS,
                        )

    # Save action types
    at_path = tmp_entry_path.replace("log_entry", "action_types")
    with open(at_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
        for name in sorted(action_type_names):
            writer.writerow({"id": deterministic_action_type_id(name), "name": name})


# ============================================================
# Parent process: merge results
# ============================================================

def merge_csv_files(tmp_paths, output, fields):
    with open(output, "w", newline="", encoding="utf-8") as final:
        writer = csv.DictWriter(final, fieldnames=fields)
        writer.writeheader()

        for tmp in tmp_paths:
            with open(tmp, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    writer.writerow(row)


def merge_action_types(tmp_action_paths, outdir):
    all_types = {}  # name -> id
    for path in tmp_action_paths:
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row["name"]
                id_ = row["id"]  # UUID as string
                if name not in all_types:
                    all_types[name] = id_
                else:
                    # If already present, assert consistency (optional)
                    if all_types[name] != id_:
                        raise ValueError(
                            f"Inconsistent UUID for action_type '{name}': "
                            f"{all_types[name]} vs {id_}"
                        )

    with open(os.path.join(outdir, ACTION_TYPE_FILENAME), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
        for name, id_ in sorted(all_types.items(), key=lambda x: x[0]):
            writer.writerow({"id": id_, "name": name})


def write_log_type_csv(outdir):
    path = os.path.join(outdir, LOG_TYPE_FILENAME)
    LOG_TYPE_IDS = load_log_type_ids()
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
        for lt, id_ in LOG_TYPE_IDS.items():
            writer.writerow({"id": id_, "name": lt.value})


# ============================================================
# Main
# ============================================================

def main(logdir="/input-logfiles", outdir="./parsed"):
    os.makedirs(outdir, exist_ok=True)
    tmpdir = os.path.join(outdir, TMP_DIRNAME)
    os.makedirs(tmpdir, exist_ok=True)

    # Log paths
    access_log = os.path.join(logdir, LogType.ACCESS.filename)
    datax_log = os.path.join(logdir, LogType.HDFS_DATAXCEIVER.filename)
    namesys_log = os.path.join(logdir, LogType.HDFS_NAMESYSTEM.filename)

    # Temp CSV files
    tmp_access_entry = os.path.join(tmpdir, "log_entry_access.csv")
    tmp_access_detail = os.path.join(tmpdir, "access_detail_access.csv")
    tmp_access_actions = os.path.join(tmpdir, "action_types_access.csv")

    tmp_datax_entry = os.path.join(tmpdir, "log_entry_datax.csv")
    tmp_datax_actions = os.path.join(tmpdir, "action_types_datax.csv")

    tmp_namesys_entry = os.path.join(tmpdir, "log_entry_namesys.csv")
    tmp_namesys_actions = os.path.join(tmpdir, "action_types_namesys.csv")

    # Parallel processes
    p1 = Process(target=parse_access_worker, args=(access_log, tmp_access_entry, tmp_access_detail))
    p2 = Process(target=parse_dataxceiver_worker, args=(datax_log, tmp_datax_entry))
    p3 = Process(target=parse_namesystem_worker, args=(namesys_log, tmp_namesys_entry))

    log("Starting workers...")
    p1.start()
    p2.start()
    p3.start()

    p1.join()
    p2.join()
    p3.join()
    log("All parsers completed.")

    # Write log_type.csv from enum
    write_log_type_csv(outdir)

    # Merge action types
    merge_action_types(
        [tmp_access_actions, tmp_datax_actions, tmp_namesys_actions],
        outdir
    )

    # Merge log_entry
    merge_csv_files(
        [tmp_access_entry, tmp_datax_entry, tmp_namesys_entry],
        os.path.join(outdir, LOG_ENTRY_FILENAME),
        ENTRY_FIELDS
    )

    # Merge access detail (only one source has real data)
    merge_csv_files(
        [tmp_access_detail],
        os.path.join(outdir, ACCESS_DETAIL_FILENAME),
        ACCESS_DETAIL_FIELDS
    )

    log("All final CSVs created successfully.")


if __name__ == "__main__":
    main()
