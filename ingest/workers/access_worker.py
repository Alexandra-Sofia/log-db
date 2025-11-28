import csv
import re
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
                    deterministic_action_type_id(action),
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

    # Save actions
    at_path = tmp_detail_path.replace("access_detail", "action_types")
    with open(at_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
        for name in sorted(action_type_names):
            writer.writerow({"id": deterministic_action_type_id(name), "name": name})
