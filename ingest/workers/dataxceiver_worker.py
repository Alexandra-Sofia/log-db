import csv
import re
from timestamps import ts_hdfs_compact
from ids import load_log_type_ids, deterministic_action_type_id
from writers import write_entry
from config import ENTRY_FIELDS
from util import LogType

DATAX_REGEX = re.compile(
    r'''
    ^(?P<date>\d{6})\s+
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
    )$''',
    re.VERBOSE
)


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

                # receiving
                if g.get("op_receiving"):
                    action = "receiving"
                    action_type_names.add(action)
                    write_entry(
                        w_entry,
                        LogType.HDFS_DATAXCEIVER,
                        deterministic_action_type_id(action),
                        ts,
                        g["src_receiving"],
                        g["dst_receiving"],
                        int(g["blk_receiving"].replace("blk_", "")),
                        "",
                        {},
                        LOG_TYPE_IDS,
                    )
                    continue

                # received
                if g.get("op_received"):
                    action = "received"
                    action_type_names.add(action)
                    size = g["size_received"]
                    write_entry(
                        w_entry,
                        LogType.HDFS_DATAXCEIVER,
                        deterministic_action_type_id(action),
                        ts,
                        g["src_received"],
                        g["dst_received"],
                        int(g["blk_received"].replace("blk_", "")),
                        int(size) if size else "",
                        {},
                        LOG_TYPE_IDS,
                    )
                    continue

                # served
                if g.get("op_served"):
                    action = "served"
                    action_type_names.add(action)
                    write_entry(
                        w_entry,
                        LogType.HDFS_DATAXCEIVER,
                        deterministic_action_type_id(action),
                        ts,
                        g["src_served"],
                        g["dst_served"],
                        int(g["blk_served"].replace("blk_", "")),
                        "",
                        {},
                        LOG_TYPE_IDS,
                    )

    # Save actions
    at_path = tmp_entry_path.replace("log_entry", "action_types")
    with open(at_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
        for name in sorted(action_type_names):
            writer.writerow({"id": deterministic_action_type_id(name), "name": name})
