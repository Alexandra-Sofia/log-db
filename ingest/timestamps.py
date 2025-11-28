from datetime import datetime


def ts_apache(s: str) -> datetime:
    """
    Parse an Apache log timestamp.

    Expected format: ``dd/Mon/yyyy:HH:MM:SS ±ZZZZ``

    :param s: Timestamp string from an Apache access log.
    :return: Parsed ``datetime`` object with timezone info.
    """
    return datetime.strptime(s, "%d/%b/%Y:%H:%M:%S %z")


def ts_hdfs_compact(date: str, time: str) -> datetime:
    """
    Parse HDFS compact date+time into a datetime.

    Date format: ``yymmdd``
    Time format: ``HHMMSS``

    :param date: Date string in compact HDFS format.
    :param time: Time string in compact HDFS format.
    :return: Parsed ``datetime`` object (naive, UTC assumed external).
    """
    return datetime.strptime(date + time, "%y%m%d%H%M%S")
