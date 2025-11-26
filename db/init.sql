CREATE TABLE IF NOT EXISTS log_type (
    id      SMALLSERIAL PRIMARY KEY,
    name    TEXT        UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS action_type (
    id      SMALLSERIAL PRIMARY KEY,
    name    TEXT        UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS log_entry (
    id              BIGSERIAL PRIMARY KEY,

    log_type_id     SMALLINT      NOT NULL,
    action_type_id  SMALLINT,

    log_timestamp   TIMESTAMPTZ   NOT NULL,
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

CREATE INDEX IF NOT EXISTS idx_log_entry_timestamp   ON log_entry (log_timestamp);
CREATE INDEX IF NOT EXISTS idx_log_entry_log_type    ON log_entry (log_type_id);
CREATE INDEX IF NOT EXISTS idx_log_entry_action_type ON log_entry (action_type_id);
CREATE INDEX IF NOT EXISTS idx_log_entry_source_ip   ON log_entry (source_ip);
CREATE INDEX IF NOT EXISTS idx_log_entry_dest_ip     ON log_entry (dest_ip);
CREATE INDEX IF NOT EXISTS idx_log_entry_block_id    ON log_entry (block_id);


CREATE TABLE IF NOT EXISTS log_access_detail (
    log_entry_id BIGINT PRIMARY KEY REFERENCES log_entry(id),
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

CREATE INDEX IF NOT EXISTS idx_access_resource   ON log_access_detail (resource);
CREATE INDEX IF NOT EXISTS idx_access_status     ON log_access_detail (http_status);
CREATE INDEX IF NOT EXISTS idx_access_method     ON log_access_detail (http_method);
CREATE INDEX IF NOT EXISTS idx_access_useragent  ON log_access_detail (user_agent);

CREATE TABLE IF NOT EXISTS app_user (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT        NOT NULL,
    login_name  TEXT        UNIQUE NOT NULL,
    password    TEXT        NOT NULL,
    address     TEXT,
    email       TEXT        UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS user_query_log (
    id           BIGSERIAL   PRIMARY KEY,
    user_id      BIGINT       NOT NULL,
    query_text   TEXT         NOT NULL,
    executed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_user_query_user
        FOREIGN KEY (user_id)
        REFERENCES app_user(id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS idx_user_query_user ON user_query_log (user_id);
