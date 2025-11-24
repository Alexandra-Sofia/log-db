#!/usr/bin/env python3
import re
import os
from datetime import datetime
from typing import Callable, Dict, List, Optional, Any

from logger import setup_logger
logger = setup_logger("log_parser")


def ts_apache(s: str) -> datetime:
    """
    Parse an Apache-style timestamp.

    :param s: Timestamp string in Apache log format.
    :return: Parsed datetime instance.
    """
    return datetime.strptime(s, "%d/%b/%Y:%H:%M:%S %z")


def ts_hdfs_compact(date: str, time: str) -> datetime:
    """
    Parse a compact HDFS timestamp composed of YYMMDD and HHMMSS.

    :param date: Six-digit date string.
    :param time: Six-digit time string.
    :return: Parsed datetime instance.
    """
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
    file_name: str,
    line_number: int,
    raw_message: str,
    detail: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Construct a dictionary representing a normalized log entry.

    :param log_type_name: Name of the log type.
    :param action_type_name: Name of the action type.
    :param log_timestamp: Parsed timestamp.
    :param source_ip: Source IP address if present.
    :param dest_ip: Destination IP address if present.
    :param block_id: Block identifier if present.
    :param size_bytes: Size field if present.
    :param file_name: Name of the log file.
    :param line_number: Line number within the log file.
    :param raw_message: Original raw log line.
    :param detail: Additional attributes for ACCESS input-logfiles.
    :return: Dictionary aligned with the target schema.
    """
    return {
        "log_type_name": log_type_name,
        "action_type_name": action_type_name,
        "log_timestamp": log_timestamp,
        "source_ip": source_ip,
        "dest_ip": dest_ip,
        "block_id": block_id,
        "size_bytes": size_bytes,
        "file_name": file_name,
        "line_number": line_number,
        "raw_message": raw_message,
        "detail": detail
    }


def parse_file(
    path: str,
    regex: re.Pattern,
    row_builder: Callable[[Dict[str, str], datetime], Any]
) -> List[Dict[str, Any]]:
    """
    Parse a text file with verbose logging for debugging regex mismatches.

    :param path: Path to the log file.
    :param regex: Regular expression describing line structure.
    :param row_builder: Function building row dictionaries from matches.
    :return: List of parsed and normalized rows.
    """
    logger.info(f"[parse_file] Starting: {path}")
    rows: List[Dict[str, Any]] = []
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

            entry_list = row_builder(g, line_no)

            for row in entry_list:
                rows.append(
                    make_row(
                        log_type_name=row["log_type_name"],
                        action_type_name=row["action_type_name"],
                        log_timestamp=row["timestamp"],
                        source_ip=row["source_ip"],
                        dest_ip=row["dest_ip"],
                        block_id=row["block_id"],
                        size_bytes=row["size_bytes"],
                        file_name=path,
                        line_number=line_no,
                        raw_message=raw,
                        detail=row["detail"]
                    )
                )

    logger.info(f"[parse_file] Finished {path}: matched {matched}/{total}")
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
    """
    Build structured ACCESS log rows.

    :param g: Regex group dictionary.
    :param _: Line number (unused).
    :return: List of one structured row.
    """
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
    r'^(?P<date>\d{6})\s+'
    r'(?P<time>\d{6})\s+'
    r'\d+\s+INFO\s+dfs\.DataNode\$DataXceiver:\s+'
    r'(?:(?P<prefix_ip>[\d\.]+):\d+\s+)?'
    r'(?P<op>Receiving|Received|Served)\s+block\s+blk_(?P<block>-?\d+)'
    r'(?:\s+src:\s*/(?P<src_ip>[\d\.]+):\d+)?'
    r'(?:\s+dest:\s*/(?P<dest_ip>[\d\.]+):\d+)?'
    r'(?:\s+to\s+/(?P<to_ip>[\d\.]+))?'
    r'(?:\s+of\s+size\s+(?P<size>\d+))?'
)


def build_datax(g: Dict[str, str], _: int) -> List[Dict[str, Any]]:
    """
    Build structured HDFS DataXceiver rows.

    :param g: Regex group dictionary.
    :param _: Line number (unused).
    :return: List of one structured row.
    """
    timestamp = ts_hdfs_compact(g["date"], g["time"])

    return [{
        "log_type_name": "HDFS_DATAXCEIVER",
        "action_type_name": g["op"].lower(),
        "timestamp": timestamp,
        "source_ip": g["src_ip"],
        "dest_ip": g["dest_ip"],
        "block_id": int(g["block"]),
        "size_bytes": None,
        "detail": None
    }]


NAMESYS_REGEX = re.compile(
    r'^(?P<date>\d{6})\s+'
    r'(?P<time>\d{6})\s+'
    r'\d+\s+INFO\s+dfs\.FSNamesystem:\s+BLOCK\*\s+'
    r'NameSystem\.(?P<op>\w+):\s+'
    r'(?:blockMap updated:\s+(?P<ip>[\d\.]+)\s+)?'
    r'blk_(?P<block>\d+)'
    r'(?:\s+size:\s+(?P<size>\d+))?'
)


def build_namesystem(g: Dict[str, str], _: int) -> List[Dict[str, Any]]:
    """
    Build structured HDFS NameSystem rows.

    :param g: Regex group dictionary.
    :param _: Line number (unused).
    :return: List of one structured row.
    """
    timestamp = ts_hdfs_compact(g["date"], g["time"])
    block_id = int(g["block"])
    size = int(g["size"]) if g["size"] else None
    ip = g["ip"] if g["ip"] else None

    return [{
        "log_type_name": "HDFS_NAMESYSTEM",
        "action_type_name": g["op"].lower(),
        "timestamp": timestamp,
        "source_ip": ip,
        "dest_ip": None,
        "block_id": block_id,
        "size_bytes": size,
        "detail": None
    }]


def parse_access(path: str) -> List[Dict[str, Any]]:
    """
    Parse ACCESS log file.

    :param path: Path to ACCESS log file.
    :return: List of structured rows.
    """
    return parse_file(path, ACCESS_REGEX, build_access)


def parse_dataxceiver(path: str) -> List[Dict[str, Any]]:
    """
    Parse DataXceiver log file.

    :param path: Path to DataXceiver log file.
    :return: List of structured rows.
    """
    return parse_file(path, DATAX_REGEX, build_datax)


def parse_namesystem(path: str) -> List[Dict[str, Any]]:
    """
    Parse NameSystem log file.

    :param path: Path to NameSystem log file.
    :return: List of structured rows.
    """
    return parse_file(path, NAMESYS_REGEX, build_namesystem)


def parse_all(logdir: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse all supported log types from a directory.

    :param logdir: Directory with log files.
    :return: Mapping of log types to parsed rows.
    """
    return {
        "access": parse_access(os.path.join(logdir, "access_log_full")),
        "dataxceiver": parse_dataxceiver(os.path.join(logdir, "HDFS_DataXceiver.log")),
        "namesystem": parse_namesystem(os.path.join(logdir, "HDFS_FS_Namesystem.log")),
    }


if __name__ == "__main__":
    data = parse_all("./input-logfiles")
    logger.info(f"ACCESS: {len(data['access'])}")
    logger.info(f"DATAX: {len(data['dataxceiver'])}")
    logger.info(f"NAMESYS: {len(data['namesystem'])}")
