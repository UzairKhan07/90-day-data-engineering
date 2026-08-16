-- Silver Layer - Facts / Transactional tables

CREATE SCHEMA IF NOT EXISTS silver;

-- silver.orders
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id                        TEXT            PRIMARY KEY,
    customer_id                     TEXT,
    order_status                    TEXT,           
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP,
    latest_logistics_status         TEXT,
    latest_status_timestamp         TIMESTAMP,
    carrier                         TEXT,
    tracking_number                 TEXT,
    warehouse_id                    TEXT,
    is_delivered                    BOOLEAN,
    is_cancelled                    BOOLEAN,
    is_delayed                      BOOLEAN,
    delivery_delay_days             INTEGER,       
    _source_system                  TEXT            NOT NULL DEFAULT 'kaggle+minio',
    _loaded_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _batch_id                       TEXT,
    _row_hash                       TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_orders_customer
    ON silver.orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_silver_orders_status
    ON silver.orders (order_status);

CREATE INDEX IF NOT EXISTS idx_silver_orders_logistics_status
    ON silver.orders (latest_logistics_status);

CREATE INDEX IF NOT EXISTS idx_silver_orders_purchase_ts
    ON silver.orders (order_purchase_timestamp);


-- silver.order_items
CREATE TABLE IF NOT EXISTS silver.order_items (
    order_id                TEXT,
    order_item_id           INTEGER,
    product_id              TEXT,
    seller_id               TEXT,
    shipping_limit_date     TIMESTAMP,
    price                   NUMERIC(12, 2),
    freight_value           NUMERIC(12, 2),
    discount_rate           NUMERIC(5, 4),
    line_total              NUMERIC(12, 2),     
    _source_system          TEXT            NOT NULL DEFAULT 'kaggle',
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _batch_id               TEXT,
    _row_hash               TEXT,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_order_items_product
    ON silver.order_items (product_id);

CREATE INDEX IF NOT EXISTS idx_silver_order_items_seller
    ON silver.order_items (seller_id);


-- silver.returns
CREATE TABLE IF NOT EXISTS silver.returns (
    return_id               TEXT            PRIMARY KEY,
    order_id                TEXT,
    product_id              TEXT,
    return_date             DATE,
    return_reason           TEXT,
    refund_amount           NUMERIC(12, 2),
    refund_currency         TEXT,
    return_status           TEXT,
    quantity                INTEGER,
    _source_system          TEXT            NOT NULL DEFAULT 'minio',
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _batch_id               TEXT,
    _row_hash               TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_returns_order
    ON silver.returns (order_id);

CREATE INDEX IF NOT EXISTS idx_silver_returns_product
    ON silver.returns (product_id);

CREATE INDEX IF NOT EXISTS idx_silver_returns_reason
    ON silver.returns (return_reason);

CREATE INDEX IF NOT EXISTS idx_silver_returns_status
    ON silver.returns (return_status);