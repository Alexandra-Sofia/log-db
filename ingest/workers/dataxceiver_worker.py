import csv
import re
from typing import Dict, Set, Any

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
    re.VERBOSE,
)


def parse_dataxceiver_worker(input_path: str, tmp_entry_path: str) -> None:
    """
    Parse HDFS DataXceiver logs and write temporary ``log_entry`` rows
    along with a temporary ``action_types`` CSV containing unique actions.

    :param input_path: Path to the raw DataXceiver log file.
    :param tmp_entry_path: Path to the temporary output log_entry CSV.
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
                match = DATAX_REGEX.match(line)
                if not match:
                    continue

                fields: Dict[str, Any] = match.groupdict()
                timestamp = ts_hdfs_compact(fields["date"], fields["time"])

                if fields.get("op_receiving"):
                    add_receiving(
                        writer_entry,
                        fields,
                        timestamp,
                        log_type_ids,
                        action_type_names,
                    )
                    continue

                if fields.get("op_received"):
                    add_received(
                        writer_entry,
                        fields,
                        timestamp,
                        log_type_ids,
                        action_type_names,
                    )
                    continue

                if fields.get("op_served"):
                    add_served(
                        writer_entry,
                        fields,
                        timestamp,
                        log_type_ids,
                        action_type_names,
                    )

    write_action_types(tmp_entry_path, action_type_names)


def add_receiving(
    writer_entry: csv.DictWriter,
    fields: Dict[str, Any],
    timestamp,
    log_type_ids: Dict[LogType, int],
    action_type_names: Set[str],
) -> None:
    """
    Write a ``receiving`` log_entry row.

    :param writer_entry: CSV DictWriter for log_entry rows.
    :param fields: Matched regex field dictionary.
    :param timestamp: Parsed timestamp object.
    :param log_type_ids: Mapping of LogType to numeric IDs.
    :param action_type_names: Set collecting unique action names.
    :return: None
    """
    action = "receiving"
    action_type_names.add(action)

    write_entry(
        writer_entry,
        LogType.HDFS_DATAXCEIVER,
        deterministic_action_type_id(action),
        timestamp,
        fields["src_receiving"],
        fields["dst_receiving"],
        int(fields["blk_receiving"].replace("blk_", "")),
        "",
        {},
        log_type_ids,
    )


def add_received(
    writer_entry: csv.DictWriter,
    fields: Dict[str, Any],
    timestamp,
    log_type_ids: Dict[LogType, int],
    action_type_names: Set[str],
) -> None:
    """
    Write a ``received`` log_entry row.

    :param writer_entry: CSV DictWriter for log_entry rows.
    :param fields: Matched regex field dictionary.
    :param timestamp: Parsed timestamp object.
    :param log_type_ids: Mapping of LogType to numeric IDs.
    :param action_type_names: Set collecting unique action names.
    :return: None
    """
    action = "received"
    action_type_names.add(action)

    size_value = int(fields["size_received"]) if fields["size_received"] else ""

    write_entry(
        writer_entry,
        LogType.HDFS_DATAXCEIVER,
        deterministic_action_type_id(action),
        timestamp,
        fields["src_received"],
        fields["dst_received"],
        int(fields["blk_received"].replace("blk_", "")),
        size_value,
        {},
        log_type_ids,
    )


def add_served(
    writer_entry: csv.DictWriter,
    fields: Dict[str, Any],
    timestamp,
    log_type_ids: Dict[LogType, int],
    action_type_names: Set[str],
) -> None:
    """
    Write a ``served`` log_entry row.

    :param writer_entry: CSV DictWriter for log_entry rows.
    :param fields: Matched regex field dictionary.
    :param timestamp: Parsed timestamp object.
    :param log_type_ids: Mapping of LogType to numeric IDs.
    :param action_type_names: Set collecting unique action names.
    :return: None
    """
    action = "served"
    action_type_names.add(action)

    write_entry(
        writer_entry,
        LogType.HDFS_DATAXCEIVER,
        deterministic_action_type_id(action),
        timestamp,
        fields["src_served"],
        fields["dst_served"],
        int(fields["blk_served"].replace("blk_", "")),
        "",
        {},
        log_type_ids,
    )


def write_action_types(
    tmp_entry_path: str,
    action_type_names: Set[str],
) -> None:
    """
    Write the unique DataXceiver action types to the matching temporary CSV.

    :param tmp_entry_path: Path of the log_entry CSV used to derive output path.
    :param action_type_names: Set of unique action names.
    :return: None
    """
    action_path = tmp_entry_path.replace("log_entry", "action_types")

    with open(action_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["id", "name"])
        writer.writeheader()

        for name in sorted(action_type_names):
            writer.writerow({
                "id": deterministic_action_type_id(name),
                "name": name,
            })
