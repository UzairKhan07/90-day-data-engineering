-- Bronze Layer - MinIO Partner Feeds

CREATE SCHEMA IF NOT EXISTS bronze;

-- bronze.order_status
CREATE TABLE IF NOT EXISTS bronze.order_status (
    order_id                TEXT,
    status                  TEXT,
    status_timestamp        TIMESTAMP,
    carrier                 TEXT,
    tracking_number         TEXT,
    warehouse_id            TEXT,
    notes                   TEXT,
    source_system           TEXT,
    file_date               DATE,
    _source_system          TEXT        NOT NULL DEFAULT 'minio',
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _batch_id               TEXT,
    _file_name              TEXT,
    _row_hash               TEXT,
    PRIMARY KEY (order_id, status_timestamp, status)
);

CREATE INDEX IF NOT EXISTS idx_bronze_order_status_status
    ON bronze.order_status (status);

CREATE INDEX IF NOT EXISTS idx_bronze_order_status_file_date
    ON bronze.order_status (file_date);

CREATE INDEX IF NOT EXISTS idx_bronze_order_status_carrier
    ON bronze.order_status (carrier);


-- bronze.returns
CREATE TABLE IF NOT EXISTS bronze.returns (
    return_id               TEXT,
    order_id                TEXT,
    product_id              TEXT,
    return_date             DATE,
    return_reason           TEXT,
    refund_amount           NUMERIC(12, 2),
    refund_currency         TEXT,
    return_status           TEXT,
    quantity                INTEGER,
    source_system           TEXT,
    file_date               DATE,
    _source_system          TEXT        NOT NULL DEFAULT 'minio',
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _batch_id               TEXT,
    _file_name              TEXT,
    _row_hash               TEXT,
    PRIMARY KEY (return_id)
);

CREATE INDEX IF NOT EXISTS idx_bronze_returns_order
    ON bronze.returns (order_id);

CREATE INDEX IF NOT EXISTS idx_bronze_returns_product
    ON bronze.returns (product_id);

CREATE INDEX IF NOT EXISTS idx_bronze_returns_reason
    ON bronze.returns (return_reason);

CREATE INDEX IF NOT EXISTS idx_bronze_returns_status
    ON bronze.returns (return_status);


-- bronze.inventory_snapshots
CREATE TABLE IF NOT EXISTS bronze.inventory_snapshots (
    product_id              TEXT,
    warehouse_id            TEXT,
    snapshot_date           DATE,
    quantity_on_hand        INTEGER,
    quantity_reserved       INTEGER,
    quantity_available      INTEGER,
    reorder_point           INTEGER,
    category                TEXT,
    is_stockout             BOOLEAN,
    source_system           TEXT,
    file_date               DATE,
    _source_system          TEXT        NOT NULL DEFAULT 'minio',
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _batch_id               TEXT,
    _file_name              TEXT,
    _row_hash               TEXT,
    PRIMARY KEY (product_id, warehouse_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_bronze_inventory_snapshot_date
    ON bronze.inventory_snapshots (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_bronze_inventory_stockout
    ON bronze.inventory_snapshots (is_stockout);