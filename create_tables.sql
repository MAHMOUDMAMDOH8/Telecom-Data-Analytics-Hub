-- Active: 1761477266238@@localhost@5432@telecom
-- Active: 1761477266238@@localhost@5432
CREATE TABLE sms_events_raw (
    event_type          VARCHAR(20)  NULL,
    sid                 VARCHAR(50)  NULL,
    "from"              JSONB NULL,
    "to"                JSONB NULL,
    body                TEXT NULL,
    status              VARCHAR(20) NULL,
    phone_number        VARCHAR(20) NULL,
    customer            VARCHAR(20) NULL,
    registration_date   DATE NULL,
    seasonal_multiplier DECIMAL(10,2) NULL,
    billing_info        JSONB NULL,
    network_metrics     JSONB NULL,
    event_timestamp     TIMESTAMP NULL,
    inserted_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE INDEX idx_sms_raw_sid ON sms_events_raw (sid);
CREATE INDEX idx_sms_raw_event_ts ON sms_events_raw (event_timestamp);

CREATE INDEX idx_sms_raw_from_gin ON sms_events_raw USING GIN ("from");
CREATE INDEX idx_sms_raw_to_gin ON sms_events_raw USING GIN ("to");


CREATE TABLE IF NOT EXISTS error_logs (
    error_id SERIAL PRIMARY KEY,
    flowfile_id VARCHAR(100),
    error_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    original_timestamp TIMESTAMP,
    retry_count INTEGER,
    failure_reason TEXT,
    failure_message TEXT,
    kafka_topic VARCHAR(200),
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    json_data JSONB,
    raw_content TEXT
);

CREATE INDEX idx_error_timestamp ON error_logs(error_timestamp);



CREATE TABLE call_events_raw (
    event_type            TEXT null ,
    sid                   TEXT null ,
    from_json             TEXT null,
    to_json               TEXT null,
    call_duration_seconds INTEGER,
    status                TEXT null,
    call_type             TEXT null,
    phone_number          TEXT null,
    customer              TEXT null,
    behavior_profile      TEXT null,
    seasonal_multiplier   NUMERIC(4,2) null,
    billing_info          TEXT null,
    qos_metrics           TEXT null,
    event_timestamp       TEXT null
);



CREATE TABLE payment_events_raw (
    event_type          TEXT,
    sid                 TEXT,
    transaction_id      TEXT ,
    invoice_number      TEXT,
    payment_type        TEXT,
    payment_method      TEXT,
    payment_amount      NUMERIC(12,2),
    status              TEXT,
    phone_number        TEXT,
    customer            TEXT,
    seasonal_multiplier NUMERIC(4,2),
    billing_info        TEXT,
    event_timestamp     TEXT
);


CREATE TABLE recharge_events_raw (
    event_type          TEXT,
    sid                 TEXT,    
    recharge_amount     NUMERIC(12,2),
    balance_before      NUMERIC(12,2),
    balance_after       NUMERIC(12,2),
    payment_method      TEXT,
    status              TEXT,
    phone_number        TEXT,
    customer            TEXT,
    seasonal_multiplier NUMERIC(4,2),
    billing_info        TEXT,
    event_timestamp     TEXT
);





CREATE TABLE support_events_raw (
    event_type              TEXT        NOT NULL,
    sid                     TEXT        NOT NULL,
    customer                TEXT        NOT NULL,
    phone_number            TEXT        NOT NULL,
    channel                 TEXT        DEFAULT 'unknown',
    reason                  TEXT        DEFAULT 'unknown',
    wait_time_seconds       NUMERIC     DEFAULT 0,
    resolution_time_seconds NUMERIC     DEFAULT 0,
    agent_id                TEXT        DEFAULT 'unknown',
    satisfaction_score      NUMERIC     DEFAULT 0,
    first_call_resolution   BOOLEAN     DEFAULT FALSE,
    escalated               BOOLEAN     DEFAULT FALSE,
    call_back_requested     BOOLEAN     DEFAULT FALSE,
    event_timestamp         TEXT        DEFAULT NOW()
);







