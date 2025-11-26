#!/usr/bin/env python3
from datetime import datetime, timezone
from enum import Enum


class LogType(str, Enum):
    ACCESS = "ACCESS"
    HDFS_DATAXCEIVER = "HDFS_DATAXCEIVER"
    HDFS_NAMESYSTEM = "HDFS_NAMESYSTEM"

    @property
    def filename(self) -> str:
        return {
            LogType.ACCESS: "access_log_full",
            LogType.HDFS_DATAXCEIVER: "HDFS_DataXceiver.log",
            LogType.HDFS_NAMESYSTEM: "HDFS_FS_Namesystem.log",
        }[self]

    @classmethod
    def list(cls):
        return list(cls)


def tiny_logger(msg: str) -> None:
    """
    Output a UTC timestamped log line.
    Format example:
        2025-11-26 15:23:11.492 | message
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{ts} | {msg}", flush=True)
