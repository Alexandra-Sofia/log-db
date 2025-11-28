import os
import csv
from config import (
    ACTION_TYPE_FILENAME,
    LOG_TYPE_FILENAME,
)
from ids import load_log_type_ids
from util import tiny_logger

def merge_csv_files(tmp_paths, output, fields):
    with open(output, "w", newline="", encoding="utf-8") as dest:
        writer = csv.DictWriter(dest, fieldnames=fields)
        writer.writeheader()

        for tmp in tmp_paths:
            with open(tmp, encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    writer.writerow(row)

def merge_action_types(tmp_action_paths, outdir):
    all_types = {}
    for path in tmp_action_paths:
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                name = row["name"]
                id_ = row["id"]
                if name not in all_types:
                    all_types[name] = id_
                elif all_types[name] != id_:
                    raise ValueError(
                        f"UUID mismatch for action '{name}': "
                        f"{all_types[name]} vs {id_}"
                    )

    out = os.path.join(outdir, ACTION_TYPE_FILENAME)
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
        for name, id_ in sorted(all_types.items()):
            writer.writerow({"id": id_, "name": name})

def write_log_type_csv(outdir):
    LOG_TYPE_IDS = load_log_type_ids()
    out = os.path.join(outdir, LOG_TYPE_FILENAME)

    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
        for lt, id_ in LOG_TYPE_IDS.items():
            writer.writerow({"id": id_, "name": lt.value})
