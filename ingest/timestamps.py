from datetime import datetime, timezone

def ts_apache(s: str) -> datetime:
    return datetime.strptime(s, "%d/%b/%Y:%H:%M:%S %z")

def ts_hdfs_compact(date: str, time: str) -> datetime:
    return datetime.strptime(date + time, "%y%m%d%H%M%S")
