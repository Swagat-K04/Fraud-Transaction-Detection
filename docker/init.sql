CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS customers (
    cc_num      TEXT PRIMARY KEY,
    first_name  TEXT,
    last_name   TEXT,
    gender      TEXT,
    street      TEXT,
    city        TEXT,
    state       TEXT,
    zip         TEXT,
    cust_lat    DOUBLE PRECISION,
    cust_long   DOUBLE PRECISION,
    job         TEXT,
    dob         DATE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    id              BIGSERIAL,
    trans_num       TEXT NOT NULL UNIQUE,
    cc_num          TEXT,
    trans_time      TIMESTAMPTZ NOT NULL,
    category        TEXT,
    merchant        TEXT,
    amt             NUMERIC(12, 2),
    merch_lat       DOUBLE PRECISION,
    merch_long      DOUBLE PRECISION,
    age             INT,
    distance        DOUBLE PRECISION,
    hour_of_day     INT,
    day_of_week     INT,
    amt_zscore      DOUBLE PRECISION,
    tx_velocity_1h  INT,
    is_fraud        BOOLEAN NOT NULL,
    fraud_score     DOUBLE PRECISION,
    risk_level      TEXT,
    ai_reasoning    TEXT,
    model_version   TEXT DEFAULT 'xgb-v1',
    kafka_partition INT,
    kafka_offset    BIGINT,
    processed_at    TIMESTAMPTZ DEFAULT NOW()
);

SELECT create_hypertable('transactions', 'trans_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_tx_fraud ON transactions(is_fraud, trans_time DESC);
CREATE INDEX IF NOT EXISTS idx_tx_cc    ON transactions(cc_num, trans_time DESC);
CREATE INDEX IF NOT EXISTS idx_tx_risk  ON transactions(risk_level, trans_time DESC);


CREATE TABLE IF NOT EXISTS kafka_offsets (
    topic            TEXT NOT NULL,
    kafka_partition  INT  NOT NULL,
    kafka_offset     BIGINT NOT NULL,
    updated_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (topic, kafka_partition)
);

CREATE MATERIALIZED VIEW IF NOT EXISTS fraud_rate_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', trans_time) AS bucket,
    COUNT(*)                          AS total,
    COUNT(*) FILTER (WHERE is_fraud)  AS fraud_count,
    AVG(amt)                          AS avg_amount,
    SUM(amt) FILTER (WHERE is_fraud)  AS fraud_amount
FROM transactions
GROUP BY bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('fraud_rate_hourly',
    start_offset => INTERVAL '2 days',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);