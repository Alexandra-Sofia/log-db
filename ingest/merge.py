import os
import csv
from typing import List, Dict

from config import ACTION_TYPE_FILENAME, LOG_TYPE_FILENAME
from ids import load_log_type_ids
from util import tiny_logger


def merge_csv_files(
    tmp_paths: List[str],
    output_path: str,
    fields: List[str],
) -> None:
    """
    Merge multiple CSV files into a single output CSV.

    Rows are appended in the order of the provided paths.

    :param tmp_paths: List of temporary CSV file paths.
    :param output_path: Destination CSV path.
    :param fields: Ordered CSV header for the output.
    :return: None
    """
    with open(output_path, "w", newline="", encoding="utf-8") as dest:
        writer = csv.DictWriter(dest, fieldnames=fields)
        writer.writeheader()

        for path in tmp_paths:
            with open(path, encoding="utf-8") as infile:
                for row in csv.DictReader(infile):
                    writer.writerow(row)


def merge_action_types(
    tmp_action_paths: List[str],
    outdir: str,
) -> None:
    """
    Merge action_type CSV fragments from worker processes.

    Ensures:
      - each unique action name appears only once
      - UUIDs for identical actions match

    :param tmp_action_paths: Paths to temporary action_type fragments.
    :param outdir: Directory where the merged CSV will be written.
    :return: None
    """
    all_types: Dict[str, str] = {}

    for path in tmp_action_paths:
        with open(path, encoding="utf-8") as infile:
            for row in csv.DictReader(infile):
                name = row["name"]
                uuid_value = row["id"]

                if name not in all_types:
                    all_types[name] = uuid_value
                elif all_types[name] != uuid_value:
                    raise ValueError(
                        f"UUID mismatch for action '{name}': "
                        f"{all_types[name]} vs {uuid_value}"
                    )

    output_path = os.path.join(outdir, ACTION_TYPE_FILENAME)
    with open(output_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["id", "name"])
        writer.writeheader()

        for name, uuid_value in sorted(all_types.items()):
            writer.writerow({"id": uuid_value, "name": name})


def write_log_type_csv(outdir: str) -> None:
    """
    Write log_type.csv containing all LogType values and their IDs.

    :param outdir: Output directory for the CSV.
    :return: None
    """
    log_type_ids = load_log_type_ids()
    output_path = os.path.join(outdir, LOG_TYPE_FILENAME)

    with open(output_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["id", "name"])
        writer.writeheader()

        for log_type, id_value in log_type_ids.items():
            writer.writerow({"id": id_value, "name": log_type.value})
