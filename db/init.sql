-- ============================================
-- LogDB Initial Schema
-- ============================================

-- Safety: create database if not already created
-- (Docker entrypoint already creates 'logdb', so this is a safeguard)
\c logdb;

-- ======================
-- USERS & QUERY LOGGING
-- ======================
CREATE TABLE IF NOT EXISTS app_user (
    user_id SERIAL PRIMARY KEY,
    full_name TEXT NOT NULL,
    login_name TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    address TEXT,
    email TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_query_log (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES app_user(user_id) ON DELETE CASCADE,
    query_name TEXT NOT NULL,
    parameters JSONB,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ======================
-- LOG TYPES
-- ======================
CREATE TABLE IF NOT EXISTS log_type (
    log_type_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

-- Prepopulate the known log types
INSERT INTO log_type (name)
VALUES
    ('access'),
    ('hdfs_xceiver'),
    ('hdfs_namesystem')
ON CONFLICT (name) DO NOTHING;

-- ======================
-- ACCESS LOGS
-- ======================
CREATE TABLE IF NOT EXISTS access_log (
    id BIGSERIAL PRIMARY KEY,
    log_type_id INT REFERENCES log_type(log_type_id),
    client_ip INET,
    remote_user TEXT,
    http_user TEXT,
    timestamp TIMESTAMP,
    method TEXT,
    resource TEXT,
    status INT,
    size INT,
    referrer TEXT,
    user_agent TEXT
);

CREATE INDEX IF NOT EXISTS idx_access_ts ON access_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_access_ip ON access_log(client_ip);
CREATE INDEX IF NOT EXISTS idx_access_method ON access_log(method);

-- ======================
-- HDFS LOGS (Both DataXceiver + NameSystem)
-- ======================
CREATE TABLE IF NOT EXISTS hdfs_log (
    id BIGSERIAL PRIMARY KEY,
    log_type_id INT REFERENCES log_type(log_type_id),
    timestamp TIMESTAMP,
    block_id TEXT,
    source_ip INET,
    dest_ip INET,
    action TEXT,
    size BIGINT
);

CREATE INDEX IF NOT EXISTS idx_hdfs_ts ON hdfs_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_hdfs_block ON hdfs_log(block_id);
CREATE INDEX IF NOT EXISTS idx_hdfs_src ON hdfs_log(source_ip);
CREATE INDEX IF NOT EXISTS idx_hdfs_dest ON hdfs_log(dest_ip);
CREATE INDEX IF NOT EXISTS idx_hdfs_action ON hdfs_log(action);

-- ======================
-- VIEWS (Optional future use)
-- ======================
-- Example: combine all logs for global querying
CREATE OR REPLACE VIEW all_logs AS
SELECT
    'access' AS type,
    timestamp,
    client_ip AS source,
    NULL AS destination,
    method AS action,
    resource AS info
FROM access_log
UNION ALL
SELECT
    'hdfs' AS type,
    timestamp,
    source_ip AS source,
    dest_ip AS destination,
    action,
    block_id AS info
FROM hdfs_log;

-- ======================
-- DONE
-- ======================
\echo 'LogDB schema initialized successfully.'
