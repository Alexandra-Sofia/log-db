# LogDB Schema Overview

The LogDB schema organizes server logs into a normalized relational structure designed for efficient querying, analytics, and integration with the project’s thirteen assignment queries. It separates generic log metadata from access specific information, uses lookup tables for consistency, and defines optimized indexes, views, and stored functions to support both ingestion and reporting workloads.

## 1. Lookup Tables

### `log_type`
Stores the category of each log entry. Predefined types:
- ACCESS
- HDFS_DATAXCEIVER
- HDFS_NAMESYSTEM

Each type has a small integer primary key.
An index on `name` accelerates lookups.

### `action_type`
Stores the semantic action associated with a log entry.
Examples include HTTP methods (for access logs) or HDFS operations.
Each action type uses a UUID primary key with a unique name.
Indexed on `name` for fast resolution during ingestion.

## 2. Main Unified Log Table

### `log_entry`
This table stores the core metadata for every log entry.
It includes:
- unique text primary key
- foreign keys to `log_type` and `action_type`
- timestamp (`TIMESTAMPTZ`)
- source and destination IP addresses
- optional HDFS block ID
- optional byte size

Indexes support:
- time based filtering
- grouping by type or action
- IP based lookups
- block based queries
- size based comparisons
- composite access method queries

## 3. Access Log Details

### `log_access_detail`
Contains HTTP specific fields for logs classified as `ACCESS`.
One to one relationship with `log_entry`.

Fields:
- remote name
- authenticated user
- requested resource
- HTTP status
- referrer
- user agent

Indexes cover resource, status, referrer, and user agent.
A trigram GIN index accelerates `ILIKE` pattern searches.

## 4. Views for Static Queries

- `v_referrers_multiple_resources`: referrers leading to multiple resources
- `v_second_most_common_resource`: second most requested resource
- `v_blocks_rep_and_serv_same_day`: HDFS blocks replicated and served same day
- `v_blocks_rep_and_serv_same_day_hour`: same logic at hourly granularity

## 5. Stored Functions

Set of SQL functions matching the thirteen assignment queries, covering:
- time range summaries
- per day aggregations
- most frequent actions per IP
- top N blocks
- browser version filtering
- HTTP method based filtering
- N distinct method detection

A PL/pgSQL function `fn_insert_new_log` unifies ingestion:
- resolves log and action type IDs
- inserts into `log_entry`
- inserts into `log_access_detail` for ACCESS logs
- returns the generated ID
