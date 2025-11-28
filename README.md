# LogDB Ingestion & Parsing Pipeline

LogDB is a high‑performance log ingestion, parsing, and database loading pipeline designed for large heterogenous server logs. It provides a complete end‑to‑end workflow that parses raw log files (ACCESS, HDFS DataXceiver, HDFS NameSystem), normalizes them into structured CSVs, and ingests them into PostgreSQL using either batch inserts or COPY.

This README describes:
- System architecture
- Directory structure
- Parsing logic
- File formats
- Ingestion workflow
- Docker Compose usage
- Schema overview
- Development notes

---

## 1. Overview

LogDB automates the ingestion of three major log categories:

1. **Apache/HTTP ACCESS logs**
2. **HDFS DataXceiver logs**
3. **HDFS NameSystem logs**

The system:
- Parses logs using parallel worker processes
- Writes structured CSVs into `parsed/`
- Normalizes log types and actions
- Generates deterministic UUIDs for action types
- Supports fast ingestion via PostgreSQL `COPY`
- Supports slower but flexible ingestion via batch inserts

The tool is fully containerized and runs inside Docker Compose.

---

## 2. Directory Structure

```
.
├── input-logfiles/
│   ├── access_log_full
│   ├── HDFS_DataXceiver.log
│   └── HDFS_FS_Namesystem.log
│
├── parsed/
│   ├── log_type.csv
│   ├── action_type.csv
│   ├── log_entry.csv
│   ├── log_access_detail.csv
│   └── tmp/
│       ├── log_entry_access.csv
│       ├── access_detail_access.csv
│       ├── log_entry_datax.csv
│       ├── log_entry_namesys.csv
│       ├── action_types_access.csv
│       ├── action_types_datax.csv
│       └── action_types_namesys.csv
│
├── app/
│   ├── parser/
│   ├── ingest/
│   └── util/
│
├── docker-compose.yml
└── README.md
```

---

## 3. Parsing Architecture

The parser runs **3 separate worker processes**:

| Worker | Input File | Output Files | Notes |
|-------|------------|--------------|-------|
| ACCESS | access_log_full | log_entry_access.csv, access_detail_access.csv, action_types_access.csv | Extracts HTTP metadata |
| DATAX | HDFS_DataXceiver.log | log_entry_datax.csv, action_types_datax.csv | Normalizes receiving, received, served ops |
| NAMESYSTEM | HDFS_FS_Namesystem.log | log_entry_namesys.csv, action_types_namesys.csv | Handles update and replicate events |

Workers write CSVs into `parsed/tmp/`.

When all workers complete, the parent process merges outputs into final CSVs.

---

## 4. Regex Extraction

### ACCESS log regex
Handles complex referrer and user‑agent cases:

```
(?P<ip>\S+) (?P<remote_name>\S+) (?P<auth_user>\S+)
\[(?P<timestamp>.+?)\]
"(?P<method>\S+) (?P<resource>\S+) \S+"
(?P<status>\d{3})
(?P<size>\S+)
"(?P<referrer>.*?)" "(?P<agent>.*?)"
```

Works with:
- `-` placeholders
- Missing referrers
- Mixed quoting style
- Agents containing spaces, parentheses, semicolons

---

## 5. Unified Log Model

Final CSV output includes:

### log_entry.csv
```
id
log_type_id
action_type_id
log_timestamp
source_ip
dest_ip
block_id
size_bytes
```

### log_access_detail.csv
```
log_entry_id
remote_name
auth_user
http_method
resource
http_status
referrer
user_agent
```

### action_type.csv
Deterministic UUIDs guarantee stable foreign keys.

### log_type.csv
Static from enum:
- ACCESS
- HDFS_DATAXCEIVER
- HDFS_NAMESYSTEM

---

## 6. PostgreSQL Schema (Optimized)

- Normalized lookup tables
- Separate ACCESS detail table
- High‑performance indexes
- No JSON fields
- COPY‑friendly structure
- Foreign keys with deferred constraints (optional)

---

## 7. Ingestion Workflow

### A. Parse logs
Executed inside the `ingest` container:

```
python parse_parallel.py
```

Produces CSVs in `parsed/`.

### B. Load CSVs into PostgreSQL

Two supported methods:

#### 1. COPY (fastest)
```
COPY log_type FROM 'log_type.csv' CSV HEADER;
COPY action_type FROM 'action_type.csv' CSV HEADER;
COPY log_entry FROM 'log_entry.csv' CSV HEADER;
COPY log_access_detail FROM 'log_access_detail.csv' CSV HEADER;
```

#### 2. Batch inserts
Uses psycopg2 `execute_values` with 100k batches.

---

## 8. Docker Compose Usage

```
docker compose up --build
```

Services:
- `postgres`: backend database
- `ingest`: parser + loader
- `django` (optional): UI for running queries

The ingest container:
1. waits for PostgreSQL
2. parses input logs
3. writes CSV files
4. uploads them to the DB

---

## 9. Running the Parser Manually

```
python parse_parallel.py --input ./input-logfiles --out ./parsed
```

---

## 10. Queries Supported

The schema supports all 13 assignment queries, including:

- logs per type per time range  
- top‑N block IDs  
- referrers linking multiple resources  
- 2nd most common resource  
- Firefox UA matches  
- source IP frequency  
- multi‑method IP matching  
- same‑day replication and serving correlation  

Indexes ensure efficient execution.

---

## 11. Limitations & Notes

- Requires all three log files present
- Deterministic UUIDs must remain unchanged
- CSVs must preserve headers when using COPY
- ACCESS details exist only for ACCESS logs

---

## 12. License

Internal academic project. No license.

---

## 13. Author

LogDB ingestion & parsing engine  
Developed by Collaborator Alex  
2025

