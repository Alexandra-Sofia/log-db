import os
from multiprocessing import Process
from typing import List

from util import LogType, tiny_logger
from config import (
    TMP_DIRNAME,
    LOG_ENTRY_FILENAME,
    ACCESS_DETAIL_FILENAME,
    ENTRY_FIELDS,
    ACCESS_DETAIL_FIELDS,
)
from workers.access_worker import parse_access_worker
from workers.dataxceiver_worker import parse_dataxceiver_worker
from workers.namesystem_worker import parse_namesystem_worker
from merge import merge_csv_files, merge_action_types, write_log_type_csv


def main(logdir: str = "/input-logfiles", outdir: str = "./parsed") -> None:
    """
    Run the complete log parsing pipeline:

      1. Prepare directories.
      2. Spawn three workers (ACCESS, DataXceiver, Namesystem).
      3. Wait for worker completion.
      4. Write log_type.csv.
      5. Merge action types from all workers.
      6. Merge log_entry CSVs.
      7. Merge access_detail CSVs.

    :param logdir: Directory containing raw log files.
    :param outdir: Directory to write parsed CSV output.
    :return: None
    """
    os.makedirs(outdir, exist_ok=True)
    tmpdir = os.path.join(outdir, TMP_DIRNAME)
    os.makedirs(tmpdir, exist_ok=True)

    access_log = os.path.join(logdir, LogType.ACCESS.filename)
    datax_log = os.path.join(logdir, LogType.HDFS_DATAXCEIVER.filename)
    namesys_log = os.path.join(logdir, LogType.HDFS_NAMESYSTEM.filename)

    tmp_access_entry = os.path.join(tmpdir, "log_entry_access.csv")
    tmp_access_detail = os.path.join(tmpdir, "access_detail_access.csv")
    tmp_access_actions = os.path.join(tmpdir, "action_types_access.csv")

    tmp_datax_entry = os.path.join(tmpdir, "log_entry_datax.csv")
    tmp_datax_actions = os.path.join(tmpdir, "action_types_datax.csv")

    tmp_namesys_entry = os.path.join(tmpdir, "log_entry_namesys.csv")
    tmp_namesys_actions = os.path.join(tmpdir, "action_types_namesys.csv")

    tiny_logger("Starting workers...")

    p1 = Process(
        target=parse_access_worker,
        args=(access_log, tmp_access_entry, tmp_access_detail),
    )
    p2 = Process(
        target=parse_dataxceiver_worker,
        args=(datax_log, tmp_datax_entry),
    )
    p3 = Process(
        target=parse_namesystem_worker,
        args=(namesys_log, tmp_namesys_entry),
    )

    p1.start()
    p2.start()
    p3.start()

    p1.join()
    p2.join()
    p3.join()

    tiny_logger("All parsers completed.")

    write_log_type_csv(outdir)

    merge_action_types(
        [tmp_access_actions, tmp_datax_actions, tmp_namesys_actions],
        outdir,
    )

    merge_csv_files(
        [tmp_access_entry, tmp_datax_entry, tmp_namesys_entry],
        os.path.join(outdir, LOG_ENTRY_FILENAME),
        ENTRY_FIELDS,
    )

    merge_csv_files(
        [tmp_access_detail],
        os.path.join(outdir, ACCESS_DETAIL_FILENAME),
        ACCESS_DETAIL_FIELDS,
    )

    tiny_logger("All final CSVs created successfully.")


if __name__ == "__main__":
    main()
