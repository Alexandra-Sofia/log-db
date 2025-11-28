import csv
import re
from typing import Dict, Set, Any

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
    re.VERBOSE,
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
    re.VERBOSE,
)


def parse_namesystem_worker(
    input_path: str,
    tmp_entry_path: str,
) -> None:
    """
    Parse HDFS FSNamesystem logs and write temporary ``log_entry`` rows
    and a corresponding temporary ``action_types`` CSV.

    Supported operations:
      update, replicate

    :param input_path: Path to the raw Namesystem log file.
    :param tmp_entry_path: Output CSV path for temporary log_entry rows.
    :return: None
    """
    log_type_ids = load_log_type_ids()
    action_type_names: Set[str] = set()

    with open(tmp_entry_path, "w", newline="", encoding="utf-8") as entry_csv:
        writer_entry = csv.DictWriter(entry_csv, fieldnames=ENTRY_FIELDS)
        writer_entry.writeheader()

        with open(input_path, encoding="utf-8") as infile:
            for raw_line in infile:
                line = raw_line.rstrip("\n")

                match_update = NAMESYS_UPDATE_REGEX.match(line)
                if match_update:
                    add_update(
                        writer_entry,
                        match_update.groupdict(),
                        log_type_ids,
                        action_type_names,
                    )
                    continue

                match_repl = NAMESYS_ASK_REPLICATE_REGEX.match(line)
                if match_repl:
                    add_replicate(
                        writer_entry,
                        match_repl.groupdict(),
                        log_type_ids,
                        action_type_names,
                    )

    write_action_types(tmp_entry_path, action_type_names)


def add_update(
    writer_entry: csv.DictWriter,
    fields: Dict[str, Any],
    log_type_ids: Dict[LogType, int],
    action_type_names: Set[str],
) -> None:
    """
    Write an ``update`` log_entry row.

    :param writer_entry: CSV DictWriter for log_entry rows.
    :param fields: Matched regex field dictionary.
    :param log_type_ids: Mapping of LogType to numeric IDs.
    :param action_type_names: Set collecting unique actions.
    :return: None
    """
    action = "update"
    action_type_names.add(action)

    timestamp = ts_hdfs_compact(fields["date"], fields["time"])
    size_value = int(fields["size"]) if fields.get("size") else ""

    write_entry(
        writer_entry,
        LogType.HDFS_NAMESYSTEM,
        deterministic_action_type_id(action),
        timestamp,
        fields["ip"],
        "",
        int(fields["block"]),
        size_value,
        {},
        log_type_ids,
    )


def add_replicate(
    writer_entry: csv.DictWriter,
    fields: Dict[str, Any],
    log_type_ids: Dict[LogType, int],
    action_type_names: Set[str],
) -> None:
    """
    Write one or more ``replicate`` log_entry rows,
    one per destination datanode.

    :param writer_entry: CSV DictWriter for log_entry rows.
    :param fields: Matched regex field dictionary.
    :param log_type_ids: Mapping of LogType to numeric IDs.
    :param action_type_names: Set collecting unique actions.
    :return: None
    """
    action = "replicate"
    action_type_names.add(action)

    timestamp = ts_hdfs_compact(fields["date"], fields["time"])
    src_ip = fields["src_ip"]
    block_id = int(fields["block"])

    for token in fields["dest_list"].split():
        if ":" not in token:
            continue

        dest_ip = token.split(":", 1)[0]

        write_entry(
            writer_entry,
            LogType.HDFS_NAMESYSTEM,
            deterministic_action_type_id(action),
            timestamp,
            src_ip,
            dest_ip,
            block_id,
            "",
            {},
            log_type_ids,
        )


def write_action_types(
    tmp_entry_path: str,
    action_type_names: Set[str],
) -> None:
    """
    Write the unique Namesystem action types into a temporary CSV.

    :param tmp_entry_path: Path of the temporary log_entry CSV.
    :param action_type_names: Set of unique action names.
    :return: None
    """
    output_path = tmp_entry_path.replace("log_entry", "action_types")

    with open(output_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["id", "name"])
        writer.writeheader()

        for name in sorted(action_type_names):
            writer.writerow({
                "id": deterministic_action_type_id(name),
                "name": name,
            })
