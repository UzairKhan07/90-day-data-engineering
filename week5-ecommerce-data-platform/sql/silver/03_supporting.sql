-- Silver Layer - Supporting tables

CREATE SCHEMA IF NOT EXISTS silver;

-- silver.inventory
CREATE TABLE IF NOT EXISTS silver.inventory (
    product_id              TEXT,
    warehouse_id            TEXT,
    snapshot_date           DATE,
    quantity_on_hand        INTEGER,
    quantity_reserved       INTEGER,
    quantity_available      INTEGER,
    reorder_point           INTEGER,
    category                TEXT,
    is_stockout             BOOLEAN,
    _source_system          TEXT            NOT NULL DEFAULT 'minio',
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _batch_id               TEXT,
    _row_hash               TEXT,
    PRIMARY KEY (product_id, warehouse_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_silver_inventory_snapshot_date
    ON silver.inventory (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_silver_inventory_stockout
    ON silver.inventory (is_stockout);


-- silver.currency_rates
CREATE TABLE IF NOT EXISTS silver.currency_rates (
    rate_date               DATE,
    base_currency           TEXT,
    target_currency         TEXT,
    rate                    NUMERIC(18, 8),
    _source_system          TEXT            NOT NULL DEFAULT 'frankfurter',
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _batch_id               TEXT,
    _row_hash               TEXT,
    PRIMARY KEY (rate_date, base_currency, target_currency)
);

CREATE INDEX IF NOT EXISTS idx_silver_currency_base
    ON silver.currency_rates (base_currency);

CREATE INDEX IF NOT EXISTS idx_silver_currency_target
    ON silver.currency_rates (target_currency);