CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.fact_exchange_rate (
    id BIGSERIAL PRIMARY KEY,
    base_currency VARCHAR(10) NOT NULL,
    target_currency VARCHAR(10) NOT NULL,
    rate NUMERIC(18,8) NOT NULL,
    reference_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_exchange_unique
    ON analytics.fact_exchange_rate (base_currency, target_currency, reference_date);

CREATE TABLE IF NOT EXISTS analytics.fact_exchange_rate_history (
    id BIGSERIAL PRIMARY KEY,
    pipeline_run_id BIGINT,
    base_currency VARCHAR(10) NOT NULL,
    target_currency VARCHAR(10) NOT NULL,
    rate NUMERIC(18,8) NOT NULL,
    reference_date DATE NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.exchange_rates (
    id BIGSERIAL PRIMARY KEY,
    currency VARCHAR(10) NOT NULL,
    rate NUMERIC(18,8) NOT NULL,
    "timestamp" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.pipeline_run_log (
    run_id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    records_loaded INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);
