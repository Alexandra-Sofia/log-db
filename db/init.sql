-- ===========================================
-- Base lookup tables
-- ===========================================

CREATE TABLE IF NOT EXISTS log_type (
    id      SMALLSERIAL PRIMARY KEY,
    name    TEXT UNIQUE NOT NULL
);

INSERT INTO log_type (name) VALUES
    ('ACCESS'),
    ('HDFS_DATAXCEIVER'),
    ('HDFS_NAMESYSTEM')
ON CONFLICT (name) DO NOTHING;


CREATE TABLE IF NOT EXISTS action_type (
    id      SMALLSERIAL PRIMARY KEY,
    name    TEXT UNIQUE NOT NULL
);


-- ===========================================
-- Staging table for COPY-based ingestion
-- ===========================================
-- This table mirrors what the parser writes to CSV.

DROP TABLE IF EXISTS log_entry_staging;

CREATE TABLE log_entry_staging (
    log_type_name     TEXT,
    action_type_name  TEXT,
    log_timestamp     TEXT,
    source_ip         TEXT,
    dest_ip           TEXT,
    block_id          TEXT,
    size_bytes        TEXT,
    detail            TEXT
);



-- ===========================================
-- Final unified table for ALL log entries
-- ===========================================

CREATE TABLE IF NOT EXISTS log_entry (
    id              BIGSERIAL PRIMARY KEY,

    log_type_id     SMALLINT NOT NULL REFERENCES log_type(id),
    action_type_id  SMALLINT REFERENCES action_type(id),

    log_timestamp   TIMESTAMPTZ NOT NULL,

    source_ip       INET,
    dest_ip         INET,

    block_id        BIGINT,
    size_bytes      BIGINT,

    detail          JSONB
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_log_entry_timestamp      ON log_entry (log_timestamp);
CREATE INDEX IF NOT EXISTS idx_log_entry_log_type       ON log_entry (log_type_id);
CREATE INDEX IF NOT EXISTS idx_log_entry_action_type    ON log_entry (action_type_id);
CREATE INDEX IF NOT EXISTS idx_log_entry_source_ip      ON log_entry (source_ip);
CREATE INDEX IF NOT EXISTS idx_log_entry_dest_ip        ON log_entry (dest_ip);
CREATE INDEX IF NOT EXISTS idx_log_entry_block_id       ON log_entry (block_id);
CREATE INDEX IF NOT EXISTS idx_log_entry_detail_gin     ON log_entry USING GIN (detail);


-- ===========================================
-- Application-level users (unchanged from before)
-- ===========================================

CREATE TABLE IF NOT EXISTS app_user (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    login_name  TEXT UNIQUE NOT NULL,
    password    TEXT NOT NULL,
    address     TEXT,
    email       TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS user_query_log (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL REFERENCES app_user(id),
    query_text  TEXT NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_query_log_user ON user_query_log (user_id);
