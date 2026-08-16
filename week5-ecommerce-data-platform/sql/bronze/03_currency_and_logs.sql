-- Bronze Layer - Currency API + Pipeline Logs

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS logs;

-- bronze.currency_rates
CREATE TABLE IF NOT EXISTS bronze.currency_rates (
    rate_date               DATE,
    base_currency           TEXT,
    target_currency         TEXT,
    rate                    NUMERIC(18, 8),
    _source_system          TEXT        NOT NULL DEFAULT 'frankfurter',
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _batch_id               TEXT,
    _row_hash               TEXT,
    PRIMARY KEY (rate_date, base_currency, target_currency)
);

CREATE INDEX IF NOT EXISTS idx_bronze_currency_base
    ON bronze.currency_rates (base_currency);

CREATE INDEX IF NOT EXISTS idx_bronze_currency_target
    ON bronze.currency_rates (target_currency);


-- logs.pipeline_runs
CREATE TABLE IF NOT EXISTS logs.pipeline_runs (
    run_id                  TEXT            PRIMARY KEY,
    dag_id                  TEXT            NOT NULL,
    task_id                 TEXT,
    source_system           TEXT,
    status                  TEXT            NOT NULL,
    started_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finished_at             TIMESTAMPTZ,
    rows_read               BIGINT,
    rows_inserted           BIGINT,
    rows_updated            BIGINT,
    rows_skipped            BIGINT,
    error_message           TEXT,
    extra_info              JSONB
);

CREATE INDEX IF NOT EXISTS idx_logs_pipeline_runs_dag
    ON logs.pipeline_runs (dag_id, started_at DESC);


-- logs.data_quality_results
CREATE TABLE IF NOT EXISTS logs.data_quality_results (
    check_id                BIGSERIAL       PRIMARY KEY,
    run_id                  TEXT,
    layer                   TEXT            NOT NULL,
    table_name              TEXT            NOT NULL,
    check_name              TEXT            NOT NULL,
    check_type              TEXT,
    status                  TEXT            NOT NULL,
    rows_checked            BIGINT,
    rows_failed             BIGINT,
    failure_percentage      NUMERIC(8, 4),
    details                 JSONB,
    checked_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_logs_dq_table
    ON logs.data_quality_results (table_name, checked_at DESC);

CREATE INDEX IF NOT EXISTS idx_logs_dq_status
    ON logs.data_quality_results (status);


-- logs.file_ingestion_log
CREATE TABLE IF NOT EXISTS logs.file_ingestion_log (
    file_id                 BIGSERIAL       PRIMARY KEY,
    source_system           TEXT            NOT NULL,
    file_name               TEXT            NOT NULL,
    file_path               TEXT,
    file_size_bytes         BIGINT,
    file_checksum           TEXT,
    status                  TEXT            NOT NULL,
    rows_loaded             BIGINT,
    loaded_at               TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    run_id                  TEXT,

    UNIQUE (source_system, file_name, file_checksum)
);