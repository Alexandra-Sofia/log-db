import csv
import re
from typing import Dict, Set, Any

from timestamps import ts_apache
from writers import write_entry
from ids import load_log_type_ids, deterministic_action_type_id
from config import ENTRY_FIELDS, ACCESS_DETAIL_FIELDS
from util import LogType

ACCESS_REGEX = re.compile(
    r'(?P<ip>\S+)\s+'
    r'(?P<remote_name>\S+)\s+'
    r'(?P<auth_user>\S+)\s+'
    r'\[(?P<timestamp>[^]]+)\]\s+'
    r'"(?P<method>[A-Za-z]+)\s+(?P<resource>[^"]+?)\s+HTTP/[^"]+"\s+'
    r'(?P<status>\d{3})\s+'
    r'(?P<size>\S+)\s+'
    r'"(?P<referrer>[^"]*)"\s+'
    r'"(?P<agent>[^"]*)"'
)


def parse_access_worker(
    input_path: str,
    tmp_entry_path: str,
    tmp_detail_path: str,
) -> None:
    """
    Parse Apache ACCESS logs and generate temporary CSV output.

    Each matched line yields:
      • one log_entry row
      • one log_access_detail row
      • one unique action_type entry (HTTP method)

    :param input_path: Path to the raw ACCESS log file.
    :param tmp_entry_path: Output CSV path for log_entry rows.
    :param tmp_detail_path: Output CSV path for log_access_detail rows.
    :return: None
    """
    log_type_ids = load_log_type_ids()
    action_type_names: Set[str] = set()

    with (
        open(tmp_entry_path, "w", newline="", encoding="utf-8") as entry_csv,
        open(tmp_detail_path, "w", newline="", encoding="utf-8") as detail_csv
    ):
        writer_entry = csv.DictWriter(entry_csv, fieldnames=ENTRY_FIELDS)
        writer_entry.writeheader()

        writer_detail = csv.DictWriter(detail_csv, fieldnames=ACCESS_DETAIL_FIELDS)
        writer_detail.writeheader()

        with open(input_path, encoding="utf-8") as infile:
            for raw_line in infile:
                line = raw_line.rstrip("\n")
                match = ACCESS_REGEX.match(line)
                if not match:
                    continue

                fields: Dict[str, Any] = match.groupdict()
                timestamp = ts_apache(fields["timestamp"])

                action = fields["method"]
                action_type_names.add(action)

                size_bytes = (
                    int(fields["size"]) if fields["size"] not in {"", "-"} else ""
                )

                entry_id = write_entry(
                    writer_entry,
                    LogType.ACCESS,
                    deterministic_action_type_id(action),
                    timestamp,
                    fields["ip"],
                    "",
                    "",
                    size_bytes,
                    {},
                    log_type_ids,
                )

                writer_detail.writerow({
                    "log_entry_id": entry_id,
                    "remote_name": fields["remote_name"],
                    "auth_user": fields["auth_user"],
                    "resource": fields["resource"],
                    "http_status": int(fields["status"]),
                    "referrer": (
                        None if fields["referrer"] == "-" else fields["referrer"]
                    ),
                    "user_agent": fields["agent"],
                })

    write_action_types(tmp_detail_path, action_type_names)


def write_action_types(
    tmp_detail_path: str,
    action_type_names: Set[str],
) -> None:
    """
    Write unique ACCESS action types (HTTP methods) to a CSV file.

    :param tmp_detail_path: Path used to derive the action_types output filename.
    :param action_type_names: Set of unique HTTP method names.
    :return: None
    """
    action_types_path = tmp_detail_path.replace("access_detail", "action_types")

    with open(action_types_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["id", "name"])
        writer.writeheader()

        for name in sorted(action_type_names):
            writer.writerow({
                "id": deterministic_action_type_id(name),
                "name": name,
            })
