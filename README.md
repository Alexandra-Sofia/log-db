# LogDB Ingestion & Quering Pipeline

This README covers:
- Directory structure
- Schema overview (link to separate documentation)
- Parsing logic
- File formats
- Ingestion workflow
- Docker Compose usage
- Web UI overview
- Development notes

---

## 1. Overview

LogDB aims to achieve a high‑performance log ingestion, parsing, and database loading pipeline designed for large heterogenous server logs. 
It provides a complete end‑to‑end workflow that parses raw log files (ACCESS, HDFS DataXceiver, HDFS NameSystem), 
normalizes them into structured CSVs, and ingests them into PostgreSQL using Postgresql COPY. 

LogDB automates the processing of three major log categories:

1. **Apache/HTTP ACCESS logs**
2. **HDFS DataXceiver logs**
3. **HDFS NameSystem logs**

The system:
- Parses logs using parallel worker processes.
- Normalizes log types and actions.
- Generates deterministic UUIDs for action types.
- Creates temporary structured CSVs.
- Supports fast ingestion via PostgreSQL `COPY`.
- Deploys a web ui that executes pre-defined queries over the log data. 

The pipeline is fully containerized and gets deployed via Docker Compose.

---

## 2. Directory Structure

```
.
├── Dockerfile
├── README.md
├── db
│   ├── ERD.jpg                 -- Darabase schema visualization
│   ├── init.sql                -- Original sql schema loaded during docker compose.
│   ├── my-queries.txt          -- Simple storage of queries, not used just for archival purposes.
│   └── README.md               -- Schema related documentation.
├── docker-compose.yml      
├── ingest                      -- Implementation of ingestion step and corresponding dockerised service.
│   ├── Dockerfile
│   ├── __init__.py
│   ├── batch_insertion/        -- Batch insertion scripts, not used only kept for performance evaluation and archival purposes.
│   ├── *.py                    -- Generic parse step implementation scripts.
│   ├── wait_for_postgres.sh    -- Shell script that waits for the postgresql container to be up and running, before executing ingest step.
│   └── workers/                -- Dedicated parsers for each specific logfile type.
├── input-logfiles/             -- Directory to put the input loglifes for parsing.
├── load_with_copy.py           -- Implementation of data insertion in db via COPY.
├── logdb_web/                  -- Django webapp configuration files.
├── manage.py                   -- Django administrative tasks script.
├── requirements.txt            -- Django container python env requirements.
└── ui/                         -- Django web app implementation (follows django default filestructure).
```

---

## 3. PostgreSQL Schema Model representation

The schema is described thoroughly in a separate (readme.md)[./db/README.md]

---

## 4. Parsing Architecture

The parser runs **3 separate worker processes** in parallel:

| Worker | Input File | Output Files | Notes |
|-------|------------|--------------|-------|
| ACCESS | access_log_full | log_entry_access.csv, access_detail_access.csv, action_types_access.csv | Extracts HTTP metadata |
| DATAX | HDFS_DataXceiver.log | log_entry_datax.csv, action_types_datax.csv | Normalizes receiving, received, served ops |
| NAMESYSTEM | HDFS_FS_Namesystem.log | log_entry_namesys.csv, action_types_namesys.csv | Handles update and replicate events |

Workers write CSVs into `parsed/tmp/` inside the ingest docker container.

When all workers complete, the parent process merges outputs into final CSVs.

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

## 5. Ingestion Workflow

### A. Parse logs
Executed inside the `ingest` container:

```
python parse.py
```

Produces CSVs in `parsed/`.

### B. Load CSVs into PostgreSQL

```
python load.py 
```

Two different approaches were implemented:

#### 1. Postgres COPY from .csv (fastest, currently in use)
```
COPY log_type FROM 'log_type.csv' CSV HEADER;
COPY action_type FROM 'action_type.csv' CSV HEADER;
COPY log_entry FROM 'log_entry.csv' CSV HEADER;
COPY log_access_detail FROM 'log_access_detail.csv' CSV HEADER;
```

#### 2. Batch inserts (archived)
Uses psycopg2 `execute_values` with 100k batches.

---

## 6. Django webapp

The app follows Django’s standard project layout and keeps to the default boilerplate wherever possible.
The Web UI supports:
- Registration and sign in of new users using django built-in authentication service.
- Dropdown menu of the 15 possible interactions with the database executing the stored functions:
  - fn_total_logs_per_action_type
  - fn_logs_per_day_for_action
  - fn_most_common_action_per_source_ip
  - fn_top_blocks_by_actions_per_day
  - fn_referrers_multiple_resources
  - fn_second_most_common_resource
  - fn_access_logs_below_size
  - fn_blocks_rep_and_serv_same_day
  - fn_blocks_rep_and_serv_same_day_hour
  - fn_access_logs_by_user_agent_version
  - fn_ips_with_method_in_range
  - fn_ips_with_two_methods_in_range
  - fn_ips_with_n_methods_in_range
  - fn_insert_new_log
  - Find all executed queries (Not a stored function because it 
  has a dependency on Django auth_user table which is not present during the deployment of init.sql)
- Each option of the dropdown menu offers the placeholders for its corresponding functions parameters.
- For every query executed by the UI, a new row is inserted in the user_query_log table.

---
## 7. Docker Compose Usage

All the necessary services for the 

```
docker compose up [--build]
```
(--build is optional, used for fresh deployments.)

Services:
- `postgres`:  backend database
- `ingest`:    parser + loader
- `django`:    UI for running queries

---

## 8. Development notes

### Deployment
- The django container occasionally fails due to an inconsistent and hard to replicate bug. In this case docker compose needs to be restarted and it works successfully.
- The db volume is persistent. For fresh deployments execute `docker compose down -v` before `docker compose up --build`.
- All three log files need to be manually copied to the input directory.

### Design 
- ACCESS details exist only for ACCESS logs.
- Overhead for supporting new log files is relatively small. Implementation needs:
  - New regex definition for parsing.
  - New worker for parsing the input logfile.
  - Mapping of new log data columns to the log_entry table and creation of new dedicated details table for unique columns.
  - Extension of load.py to COPY the data for the new details table.

---

## 9. License

Internal academic project. No license.

---

## 10. Author

PostgresSQL Schema Design, LogDB ingestion & parsing engine, Django web UI
developed by Sofia, Alexandra 
2025

