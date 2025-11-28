#!/usr/bin/env python3
from datetime import datetime, timezone
from enum import Enum
from typing import List


class LogType(str, Enum):
    """
    Enumeration of supported log types.

    Each enum member provides:
      - a canonical string value
      - a ``filename`` property for locating the raw log file
    """

    ACCESS = "ACCESS"
    HDFS_DATAXCEIVER = "HDFS_DATAXCEIVER"
    HDFS_NAMESYSTEM = "HDFS_NAMESYSTEM"

    @property
    def filename(self) -> str:
        """
        Return the expected filename for this log type.

        :return: File name associated with the log type.
        """
        return {
            LogType.ACCESS: "access_log_full",
            LogType.HDFS_DATAXCEIVER: "HDFS_DataXceiver.log",
            LogType.HDFS_NAMESYSTEM: "HDFS_FS_Namesystem.log",
        }[self]

    @classmethod
    def list(cls) -> List["LogType"]:
        """
        Return all LogType values as a list.

        :return: List of LogType enum members.
        """
        return list(cls)


def tiny_logger(msg: str) -> None:
    """
    Print a timestamped log message in UTC.

    Format example:
        2025-11-26 15:23:11.492 | message

    :param msg: The message to output.
    :return: None
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{ts} | {msg}", flush=True)
