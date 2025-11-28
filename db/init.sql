BEGIN;

-- ============================================================
-- Extensions
-- ============================================================
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================
-- Lookup tables
-- ============================================================

CREATE TABLE IF NOT EXISTS log_type (
    id      SMALLSERIAL PRIMARY KEY NOT NULL,
    name    TEXT UNIQUE NOT NULL
);

INSERT INTO log_type (name) VALUES
    ('ACCESS'),
    ('HDFS_DATAXCEIVER'),
    ('HDFS_NAMESYSTEM')
ON CONFLICT (name) DO NOTHING;


CREATE TABLE IF NOT EXISTS action_type (
    id      UUID PRIMARY KEY NOT NULL,
    name    TEXT UNIQUE NOT NULL
);

-- Helpful indexes for lookups by name
CREATE INDEX IF NOT EXISTS idx_log_type_name
    ON log_type (name);

CREATE INDEX IF NOT EXISTS idx_action_type_name
    ON action_type (name);


-- ============================================================
-- Main unified log table
-- ============================================================

CREATE TABLE IF NOT EXISTS log_entry (
    id              TEXT PRIMARY KEY NOT NULL,

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

-- Core indexes for time and type based queries (Q1, Q2, Q3, Q4, Q8–Q13)
CREATE INDEX IF NOT EXISTS idx_log_entry_log_type_ts
    ON log_entry (log_type_id, log_timestamp);

CREATE INDEX IF NOT EXISTS idx_log_entry_action_ts
    ON log_entry (action_type_id, log_timestamp);

CREATE INDEX IF NOT EXISTS idx_log_entry_ts
    ON log_entry (log_timestamp);

-- Source/dest IP and block based queries (Q3, Q4, Q11–Q13)
CREATE INDEX IF NOT EXISTS idx_log_entry_source_ts
    ON log_entry (source_ip, log_timestamp);

CREATE INDEX IF NOT EXISTS idx_log_entry_dest_ts
    ON log_entry (dest_ip, log_timestamp);

CREATE INDEX IF NOT EXISTS idx_log_entry_block_ts
    ON log_entry (block_id, log_timestamp);

-- Size based queries (Q7)
CREATE INDEX IF NOT EXISTS idx_log_entry_size_bytes
    ON log_entry (size_bytes);

-- Composite index for access queries (Q11–Q13)
CREATE INDEX IF NOT EXISTS idx_log_entry_access_methods
    ON log_entry (log_type_id, action_type_id, log_timestamp, source_ip);


-- ============================================================
-- ACCESS detail table
-- ============================================================

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

-- Indexes for HTTP and browser queries (Q5, Q6, Q7, Q10)
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

-- Trigram index for ILIKE %pattern% on user_agent (Q10)
CREATE INDEX IF NOT EXISTS idx_access_user_agent_trgm
    ON log_access_detail
    USING GIN (user_agent gin_trgm_ops);


-- ============================================================
-- VIEWS FOR “STATIC” QUERIES (no parameters)
-- (Q5, Q6, Q8, Q9)
-- ============================================================

-- Q5. Referrers that have led to more than one resource
CREATE OR REPLACE VIEW v_referrers_multiple_resources AS
SELECT
    lad.referrer AS referrer,
    COUNT(lad.resource) AS resource_frequency
FROM log_access_detail lad
WHERE lad.referrer IS NOT NULL
GROUP BY lad.referrer
HAVING COUNT(lad.resource) > 1;

-- Q6. Second most common resource requested
CREATE OR REPLACE VIEW v_second_most_common_resource AS
SELECT resource, frequency
FROM (
    SELECT
        lad.resource,
        COUNT(*) AS frequency,
        RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk
    FROM log_access_detail lad
    GROUP BY lad.resource
) t
WHERE t.rnk = 2;

-- Q8. Blocks replicated and served on the same day
CREATE OR REPLACE VIEW v_blocks_rep_and_serv_same_day AS
SELECT
    le.block_id
FROM log_entry le
JOIN action_type at2
    ON le.action_type_id = at2.id
   AND at2.name IN ('replicate', 'served')
GROUP BY
    le.block_id,
    le.log_timestamp::date
HAVING COUNT(DISTINCT at2.name) > 1
ORDER BY le.block_id DESC;

-- Q9. Blocks replicated and served on the same day and hour
CREATE OR REPLACE VIEW v_blocks_rep_and_serv_same_day_hour AS
SELECT
    le.block_id
FROM log_entry le
JOIN action_type at2
    ON le.action_type_id = at2.id
   AND at2.name IN ('replicate', 'served')
GROUP BY
    le.block_id,
    le.log_timestamp::date,
    EXTRACT(HOUR FROM le.log_timestamp)
HAVING COUNT(DISTINCT at2.name) > 1
ORDER BY le.block_id DESC;


-- ============================================================
-- STORED FUNCTIONS FOR PARAMETERIZED QUERIES
-- (matching your 13 assignment queries)
-- ============================================================

-- Q1. Total logs per action type in a time range
CREATE OR REPLACE FUNCTION fn_total_logs_per_action_type(
    p_start TIMESTAMPTZ,
    p_end   TIMESTAMPTZ
)
RETURNS TABLE(action_type_name TEXT, logs BIGINT)
LANGUAGE sql
STABLE
AS $$
SELECT
    at.name AS action_type_name,
    COUNT(*)::BIGINT AS logs
FROM log_entry le
JOIN action_type at
  ON at.id = le.action_type_id
WHERE le.log_timestamp BETWEEN p_start AND p_end
GROUP BY at.name
ORDER BY logs DESC;
$$;


-- Q2. Total logs per day for a specific action type and time range
CREATE OR REPLACE FUNCTION fn_logs_per_day_for_action(
    p_action_name TEXT,
    p_start       TIMESTAMPTZ,
    p_end         TIMESTAMPTZ
)
RETURNS TABLE(day DATE, total_logs BIGINT)
LANGUAGE sql
STABLE
AS $$
SELECT
    le.log_timestamp::date AS day,
    COUNT(*)::BIGINT       AS total_logs
FROM log_entry le
JOIN action_type at
  ON le.action_type_id = at.id
WHERE at.name = p_action_name
  AND le.log_timestamp BETWEEN p_start AND p_end
GROUP BY le.log_timestamp::date
ORDER BY total_logs DESC;
$$;


-- Q3. Most common log (action type) per source IP for a specific day
CREATE OR REPLACE FUNCTION fn_most_common_action_per_source_ip(
    p_day DATE
)
RETURNS TABLE(source_ip INET, action_type_name TEXT, frequency BIGINT)
LANGUAGE sql
STABLE
AS $$
WITH ranked AS (
    SELECT
        le.source_ip,
        at.name AS action_type_name,
        COUNT(*)::BIGINT AS frequency,
        RANK() OVER (
            PARTITION BY le.source_ip
            ORDER BY COUNT(*) DESC
        ) AS rnk
    FROM log_entry le
    JOIN action_type at
      ON le.action_type_id = at.id
    WHERE le.log_timestamp::date = p_day
    GROUP BY le.source_ip, at.name
)
SELECT
    source_ip,
    action_type_name,
    frequency
FROM ranked
WHERE rnk = 1
ORDER BY frequency DESC;
$$;


-- Q4. Top-N block IDs by total number of actions per day in a date range
CREATE OR REPLACE FUNCTION fn_top_blocks_by_actions_per_day(
    p_start DATE,
    p_end   DATE,
    p_limit INTEGER DEFAULT 5
)
RETURNS TABLE(block_id BIGINT, day DATE, total_actions BIGINT)
LANGUAGE sql
STABLE
AS $$
SELECT
    le.block_id AS block_id,
    le.log_timestamp::date AS day,
    COUNT(le.action_type_id)::BIGINT AS total_actions
FROM log_entry le
WHERE le.log_timestamp::date >= p_start
  AND le.log_timestamp::date <  p_end
  AND le.block_id IS NOT NULL
GROUP BY le.block_id, le.log_timestamp::date
ORDER BY total_actions DESC
LIMIT p_limit;
$$;


-- Q5. Referrers that led to more than one resource (wrapper over view)
CREATE OR REPLACE FUNCTION fn_referrers_multiple_resources()
RETURNS TABLE(referrer TEXT, resource_frequency BIGINT)
LANGUAGE sql
STABLE
AS $$
SELECT referrer, resource_frequency
FROM v_referrers_multiple_resources;
$$;


-- Q6. Second most common resource requested (wrapper over view)
CREATE OR REPLACE FUNCTION fn_second_most_common_resource()
RETURNS TABLE(resource TEXT, frequency BIGINT)
LANGUAGE sql
STABLE
AS $$
SELECT resource, frequency
FROM v_second_most_common_resource;
$$;


-- Q7. Access logs (joined) where size_bytes < specified number
CREATE OR REPLACE FUNCTION fn_access_logs_below_size(
    p_max_size BIGINT
)
RETURNS TABLE(
    id              TEXT,
    log_type_id     SMALLINT,
    action_type_id  UUID,
    log_timestamp   TIMESTAMPTZ,
    source_ip       INET,
    dest_ip         INET,
    block_id        BIGINT,
    size_bytes      BIGINT,
    remote_name     TEXT,
    auth_user       TEXT,
    http_method     TEXT,
    resource        TEXT,
    http_status     INT,
    referrer        TEXT,
    user_agent      TEXT
)
LANGUAGE sql
STABLE
AS $$
SELECT
    le.id,
    le.log_type_id,
    le.action_type_id,
    le.log_timestamp,
    le.source_ip,
    le.dest_ip,
    le.block_id,
    le.size_bytes,
    lad.remote_name,
    lad.auth_user,
    lad.http_method,
    lad.resource,
    lad.http_status,
    lad.referrer,
    lad.user_agent
FROM log_entry le
JOIN log_access_detail lad
  ON le.id = lad.log_entry_id
WHERE le.size_bytes IS NOT NULL
  AND le.size_bytes < p_max_size;
$$;


-- Q8. Blocks replicated and served same day (wrapper over view)
CREATE OR REPLACE FUNCTION fn_blocks_rep_and_serv_same_day()
RETURNS TABLE(block_id BIGINT)
LANGUAGE sql
STABLE
AS $$
SELECT block_id
FROM v_blocks_rep_and_serv_same_day;
$$;


-- Q9. Blocks replicated and served same day and hour (wrapper over view)
CREATE OR REPLACE FUNCTION fn_blocks_rep_and_serv_same_day_hour()
RETURNS TABLE(block_id BIGINT)
LANGUAGE sql
STABLE
AS $$
SELECT block_id
FROM v_blocks_rep_and_serv_same_day_hour;
$$;


-- Q10. Access logs that specified a particular browser pattern
--       (e.g., '%Mozilla/6.0%'), joined entry + access detail
CREATE OR REPLACE FUNCTION fn_access_logs_by_user_agent_version(
    p_version TEXT
)
RETURNS TABLE(
    id              TEXT,
    log_type_id     SMALLINT,
    action_type_id  UUID,
    log_timestamp   TIMESTAMPTZ,
    source_ip       INET,
    dest_ip         INET,
    block_id        BIGINT,
    size_bytes      BIGINT,
    remote_name     TEXT,
    auth_user       TEXT,
    http_method     TEXT,
    resource        TEXT,
    http_status     INT,
    referrer        TEXT,
    user_agent      TEXT
)
LANGUAGE sql
STABLE
AS $$
SELECT
    le.id,
    le.log_type_id,
    le.action_type_id,
    le.log_timestamp,
    le.source_ip,
    le.dest_ip,
    le.block_id,
    le.size_bytes,
    lad.remote_name,
    lad.auth_user,
    lad.http_method,
    lad.resource,
    lad.http_status,
    lad.referrer,
    lad.user_agent
FROM log_entry le
JOIN log_access_detail lad
  ON le.id = lad.log_entry_id
WHERE lad.user_agent ILIKE ('%Mozilla/' || p_version || '%');
$$;


-- Q11. IPs that have issued a particular HTTP method in a time range
CREATE OR REPLACE FUNCTION fn_ips_with_method_in_range(
    p_log_type_name TEXT,   -- typically 'ACCESS'
    p_http_method   TEXT,   -- e.g., 'GET'
    p_start         TIMESTAMPTZ,
    p_end           TIMESTAMPTZ
)
RETURNS TABLE(source_ip INET)
LANGUAGE sql
STABLE
AS $$
SELECT DISTINCT le.source_ip
FROM log_entry le
JOIN log_type lt
  ON lt.id = le.log_type_id
JOIN action_type at
  ON at.id = le.action_type_id
WHERE lt.name = p_log_type_name
  AND at.name = p_http_method
  AND le.log_timestamp >= p_start
  AND le.log_timestamp <  p_end;
$$;


-- Q12. IPs that have issued two particular HTTP methods in a time range
CREATE OR REPLACE FUNCTION fn_ips_with_two_methods_in_range(
    p_log_type_name TEXT,   -- typically 'ACCESS'
    p_method1       TEXT,   -- e.g., 'GET'
    p_method2       TEXT,   -- e.g., 'POST'
    p_start         TIMESTAMPTZ,
    p_end           TIMESTAMPTZ
)
RETURNS TABLE(source_ip INET)
LANGUAGE sql
STABLE
AS $$
SELECT
    le.source_ip
FROM log_entry le
JOIN log_type lt
  ON lt.id = le.log_type_id
JOIN action_type at
  ON at.id = le.action_type_id
WHERE lt.name = p_log_type_name
  AND at.name IN (p_method1, p_method2)
  AND le.log_timestamp >= p_start
  AND le.log_timestamp <  p_end
GROUP BY le.source_ip
HAVING COUNT(DISTINCT at.name) = 2;
$$;


-- Q13. IPs that have issued N distinct HTTP methods in a time range
--      (use p_required_methods = 4 for “any four distinct HTTP methods”)
CREATE OR REPLACE FUNCTION fn_ips_with_n_methods_in_range(
    p_log_type_name     TEXT,   -- typically 'ACCESS'
    p_required_methods  INTEGER, -- e.g., 4
    p_start             TIMESTAMPTZ,
    p_end               TIMESTAMPTZ
)
RETURNS TABLE(source_ip INET, cnt INTEGER, methods TEXT)
LANGUAGE sql
STABLE
AS $$
SELECT
    le.source_ip,
    COUNT(DISTINCT at.name) AS cnt,
    STRING_AGG(DISTINCT at.name, '|' ORDER BY at.name) AS methods
FROM log_entry le
JOIN log_type lt
  ON lt.id = le.log_type_id
JOIN action_type at
  ON at.id = le.action_type_id
WHERE lt.name = p_log_type_name
  AND le.log_timestamp >= p_start
  AND le.log_timestamp <  p_end
GROUP BY le.source_ip
HAVING COUNT(DISTINCT at.name) = p_required_methods
ORDER BY cnt DESC;
$$;


CREATE OR REPLACE FUNCTION fn_insert_log(
    p_log_type_name     TEXT,
    p_action_type_name  TEXT,
    p_log_timestamp     TIMESTAMPTZ,
    p_source_ip         INET,
    p_dest_ip           INET,
    p_block_id          BIGINT,
    p_size_bytes        BIGINT,

    -- ACCESS–specific optional fields:
    p_remote_name       TEXT DEFAULT NULL,
    p_auth_user         TEXT DEFAULT NULL,
    p_http_method       TEXT DEFAULT NULL,
    p_resource          TEXT DEFAULT NULL,
    p_http_status       INT  DEFAULT NULL,
    p_referrer          TEXT DEFAULT NULL,
    p_user_agent        TEXT DEFAULT NULL
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_log_type_id     SMALLINT;
    v_action_type_id  UUID;
    v_new_id          TEXT;
BEGIN
    -- Look up log_type
    SELECT id INTO v_log_type_id
    FROM log_type
    WHERE name = p_log_type_name;

    IF v_log_type_id IS NULL THEN
        RAISE EXCEPTION 'Unknown log_type: %', p_log_type_name;
    END IF;

    -- Look up action_type
    SELECT id INTO v_action_type_id
    FROM action_type
    WHERE name = p_action_type_name;

    IF v_action_type_id IS NULL THEN
        RAISE EXCEPTION 'Unknown action_type: %', p_action_type_name;
    END IF;

    -- Generate text primary key (consistent with your schema)
    v_new_id := encode(gen_random_bytes(16), 'hex');

    -- Insert into main log_entry
    INSERT INTO log_entry (
        id,
        log_type_id,
        action_type_id,
        log_timestamp,
        source_ip,
        dest_ip,
        block_id,
        size_bytes
    )
    VALUES (
        v_new_id,
        v_log_type_id,
        v_action_type_id,
        p_log_timestamp,
        p_source_ip,
        p_dest_ip,
        p_block_id,
        p_size_bytes
    );

    -- If it's an ACCESS type, insert into log_access_detail
    IF p_log_type_name = 'ACCESS' THEN
        INSERT INTO log_access_detail (
            log_entry_id,
            remote_name,
            auth_user,
            http_method,
            resource,
            http_status,
            referrer,
            user_agent
        )
        VALUES (
            v_new_id,
            p_remote_name,
            p_auth_user,
            p_http_method,
            p_resource,
            p_http_status,
            p_referrer,
            p_user_agent
        );
    END IF;

    RETURN v_new_id;
END;
$$;

COMMIT;
