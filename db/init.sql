-- ============================================================
-- LOGDB SCHEMA OPTIMIZED FOR THE 13 ASSIGNMENT QUERIES
-- ============================================================

-- -------------------------
-- Lookup tables
-- -------------------------

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
    id      UUID PRIMARY KEY,
    name    TEXT UNIQUE NOT NULL
);


-- -------------------------
-- Main unified log table
-- -------------------------

CREATE TABLE IF NOT EXISTS log_entry (
    id              TEXT PRIMARY KEY,

    log_type_id     SMALLINT NOT NULL,
    action_type_id  UUID,

    log_timestamp   TIMESTAMPTZ NOT NULL,

    source_ip       INET,
    dest_ip         INET,

    block_id        BIGINT,
    size_bytes      BIGINT,

    CONSTRAINT fk_log_type
        FOREIGN KEY (log_type_id)
        REFERENCES log_type(id)
        DEFERRABLE INITIALLY DEFERRED,

    CONSTRAINT fk_action_type
        FOREIGN KEY (action_type_id)
        REFERENCES action_type(id)
        DEFERRABLE INITIALLY DEFERRED
);

-- Core indexes for time and type based queries
CREATE INDEX IF NOT EXISTS idx_log_entry_log_type_ts
    ON log_entry (log_type_id, log_timestamp);

CREATE INDEX IF NOT EXISTS idx_log_entry_action_ts
    ON log_entry (action_type_id, log_timestamp);

CREATE INDEX IF NOT EXISTS idx_log_entry_ts
    ON log_entry (log_timestamp);

-- Source/dest IP and block based queries
CREATE INDEX IF NOT EXISTS idx_log_entry_source_ts
    ON log_entry (source_ip, log_timestamp);

CREATE INDEX IF NOT EXISTS idx_log_entry_dest_ts
    ON log_entry (dest_ip, log_timestamp);

CREATE INDEX IF NOT EXISTS idx_log_entry_block_ts
    ON log_entry (block_id, log_timestamp);


-- -------------------------
-- ACCESS detail table
-- -------------------------

CREATE TABLE IF NOT EXISTS log_access_detail (
    log_entry_id TEXT PRIMARY KEY,

    remote_name  TEXT,
    auth_user    TEXT,
    http_method  TEXT,
    resource     TEXT,
    http_status  INT,
    referrer     TEXT,
    user_agent   TEXT,

    CONSTRAINT fk_access_detail_entry
        FOREIGN KEY (log_entry_id)
        REFERENCES log_entry(id)
        DEFERRABLE INITIALLY DEFERRED
);

-- Indexes for HTTP and browser queries
CREATE INDEX IF NOT EXISTS idx_access_method
    ON log_access_detail (http_method);

CREATE INDEX IF NOT EXISTS idx_access_resource
    ON log_access_detail (resource);

CREATE INDEX IF NOT EXISTS idx_access_status
    ON log_access_detail (http_status);

CREATE INDEX IF NOT EXISTS idx_access_referrer
    ON log_access_detail (referrer);

CREATE INDEX IF NOT EXISTS idx_access_user_agent
    ON log_access_detail (user_agent);

