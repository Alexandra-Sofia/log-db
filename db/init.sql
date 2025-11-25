-- ===========================================
--  Base lookup tables
-- ===========================================

CREATE TABLE log_type (
    id      SMALLSERIAL PRIMARY KEY,
    name    TEXT UNIQUE NOT NULL
);

INSERT INTO log_type (name) VALUES
    ('ACCESS'),
    ('HDFS_DATAXCEIVER'),
    ('HDFS_NAMESYSTEM');


CREATE TABLE action_type (
    id      SMALLSERIAL PRIMARY KEY,
    name    TEXT UNIQUE NOT NULL
);


-- ===========================================
--  Main unified table for ALL log entries
-- ===========================================

CREATE TABLE log_entry (
    id              BIGSERIAL PRIMARY KEY,

    log_type_id     SMALLINT NOT NULL REFERENCES log_type(id),
    action_type_id  SMALLINT REFERENCES action_type(id),

    log_timestamp   TIMESTAMPTZ NOT NULL,

    source_ip       INET,
    dest_ip         INET,

    block_id        BIGINT,
    size_bytes      BIGINT,

    file_name       TEXT,
    line_number     INTEGER,

    raw_message     TEXT NOT NULL
);

-- Indexes for performance
CREATE INDEX idx_log_entry_timestamp      ON log_entry (log_timestamp);
CREATE INDEX idx_log_entry_log_type       ON log_entry (log_type_id);
CREATE INDEX idx_log_entry_action_type    ON log_entry (action_type_id);
CREATE INDEX idx_log_entry_source_ip      ON log_entry (source_ip);
CREATE INDEX idx_log_entry_dest_ip        ON log_entry (dest_ip);
CREATE INDEX idx_log_entry_block_id       ON log_entry (block_id);


-- ===========================================
--  Access-log-only details (1-to-1)
-- ===========================================

CREATE TABLE log_access_detail (
    log_entry_id    BIGINT PRIMARY KEY REFERENCES log_entry(id) ON DELETE CASCADE,

    remote_name     TEXT,
    auth_user       TEXT,
    http_method     TEXT,
    resource        TEXT,
    http_status     INTEGER,
    referrer        TEXT,
    user_agent      TEXT
);

CREATE INDEX idx_access_http_status ON log_access_detail (http_status);
CREATE INDEX idx_access_method      ON log_access_detail (http_method);
CREATE INDEX idx_access_resource    ON log_access_detail (resource);


-- ===========================================
--  Application-level users
-- ===========================================

CREATE TABLE app_user (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    login_name  TEXT UNIQUE NOT NULL,
    password    TEXT NOT NULL,
    address     TEXT,
    email       TEXT UNIQUE NOT NULL
);

CREATE TABLE user_query_log (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL REFERENCES app_user(id),
    query_text  TEXT NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_user_query_log_user ON user_query_log (user_id);
