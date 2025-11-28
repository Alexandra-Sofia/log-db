import uuid

def write_entry(writer_entry, log_type, action, timestamp,
                source_ip, dest_ip, block_id, size_bytes,
                detail, LOG_TYPE_IDS):
    entry_id = str(uuid.uuid4())
    writer_entry.writerow({
        "id": entry_id,
        "log_type_id": LOG_TYPE_IDS[log_type],
        "action_type_id": action,
        "log_timestamp": timestamp.isoformat(),
        "source_ip": source_ip,
        "dest_ip": dest_ip,
        "block_id": block_id if block_id != "" else "",
        "size_bytes": size_bytes if size_bytes != "" else "",
    })
    return entry_id
