import csv
import re
from timestamps import ts_hdfs_compact
from ids import load_log_type_ids, deterministic_action_type_id
from writers import write_entry
from config import ENTRY_FIELDS
from util import LogType

NAMESYS_UPDATE_REGEX = re.compile(
    r'''
    ^(?P<date>\d{6})\s+
    (?P<time>\d{6})\s+
    (?P<tid>\d+)\s+
    INFO\s+dfs\.FSNamesystem:\s+BLOCK\*\s+
    NameSystem\.\w+:\s+
    blockMap updated:\s+
    (?P<ip>[0-9.]+):\d+.*?
    blk_(?P<block>-?\d+)
    (?:\s+size\s+(?P<size>\d+))?
    $''',
    re.VERBOSE
)

NAMESYS_ASK_REPLICATE_REGEX = re.compile(
    r'''
    ^(?P<date>\d{6})\s+
    (?P<time>\d{6})\s+
    (?P<tid>\d+)\s+
    INFO\s+dfs\.FSNamesystem:\s+BLOCK\*\s+
    ask\s+(?P<src_ip>[0-9.]+):\d+
    \s+to\s+replicate\s+
    blk_(?P<block>-?\d+)
    \s+to\s+datanode\(s\)\s+
    (?P<dest_list>(?:[0-9.]+:\d+\s*)+)
    $''',
    re.VERBOSE
)

def parse_namesystem_worker(input_path, tmp_entry_path):
    LOG_TYPE_IDS = load_log_type_ids()
    action_type_names = set()

    with open(tmp_entry_path, "w", newline="", encoding="utf-8") as entry_csv:
        w_entry = csv.DictWriter(entry_csv, fieldnames=ENTRY_FIELDS)
        w_entry.writeheader()

        with open(input_path, encoding="utf-8") as f:
            for raw in f:
                line = raw.rstrip("\n")

                upd = NAMESYS_UPDATE_REGEX.match(line)
                if upd:
                    g = upd.groupdict()
                    ts = ts_hdfs_compact(g["date"], g["time"])
                    action = "update"
                    action_type_names.add(action)
                    write_entry(
                        w_entry,
                        LogType.HDFS_NAMESYSTEM,
                        deterministic_action_type_id(action),
                        ts,
                        g["ip"],
                        "",
                        int(g["block"]),
                        int(g["size"]) if g.get("size") else "",
                        {},
                        LOG_TYPE_IDS,
                    )
                    continue

                rep = NAMESYS_ASK_REPLICATE_REGEX.match(line)
                if rep:
                    g = rep.groupdict()
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
                            deterministic_action_type_id(action),
                            ts,
                            src_ip,
                            dest_ip,
                            block_id,
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
