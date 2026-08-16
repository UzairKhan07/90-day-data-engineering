-- Gold Layer - Facts

CREATE SCHEMA IF NOT EXISTS gold;

-- gold.fact_orders
CREATE TABLE IF NOT EXISTS gold.fact_orders (
    order_id                        TEXT            PRIMARY KEY,
    customer_id                     TEXT,
    order_purchase_date             DATE,
    order_status                    TEXT,
    latest_logistics_status         TEXT,
    carrier                         TEXT,
    warehouse_id                    TEXT,
    is_delivered                    BOOLEAN,
    is_cancelled                    BOOLEAN,
    is_delayed                      BOOLEAN,
    delivery_delay_days             INTEGER,
    order_item_count                INTEGER,
    order_gross_value               NUMERIC(14, 2),   
    order_freight_value             NUMERIC(14, 2),
    order_net_value                 NUMERIC(14, 2),   
    has_return                      BOOLEAN         DEFAULT FALSE,
    return_count                    INTEGER         DEFAULT 0,
    total_refund_amount             NUMERIC(14, 2)  DEFAULT 0,
    _loaded_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_fact_orders_date
    ON gold.fact_orders (order_purchase_date);

CREATE INDEX IF NOT EXISTS idx_gold_fact_orders_customer
    ON gold.fact_orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_gold_fact_orders_status
    ON gold.fact_orders (order_status);

CREATE INDEX IF NOT EXISTS idx_gold_fact_orders_logistics
    ON gold.fact_orders (latest_logistics_status);


-- gold.fact_order_items
CREATE TABLE IF NOT EXISTS gold.fact_order_items (
    order_id                TEXT,
    order_item_id           INTEGER,
    product_id              TEXT,
    seller_id               TEXT,
    order_purchase_date     DATE,
    price                   NUMERIC(12, 2),
    freight_value           NUMERIC(12, 2),
    discount_rate           NUMERIC(5, 4),
    line_total              NUMERIC(12, 2),
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE INDEX IF NOT EXISTS idx_gold_fact_order_items_product
    ON gold.fact_order_items (product_id);

CREATE INDEX IF NOT EXISTS idx_gold_fact_order_items_date
    ON gold.fact_order_items (order_purchase_date);


-- gold.fact_returns
CREATE TABLE IF NOT EXISTS gold.fact_returns (
    return_id               TEXT            PRIMARY KEY,
    order_id                TEXT,
    product_id              TEXT,
    return_date             DATE,
    return_reason           TEXT,
    return_status           TEXT,
    quantity                INTEGER,
    refund_amount           NUMERIC(12, 2),
    refund_currency         TEXT,
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_fact_returns_date
    ON gold.fact_returns (return_date);

CREATE INDEX IF NOT EXISTS idx_gold_fact_returns_reason
    ON gold.fact_returns (return_reason);

CREATE INDEX IF NOT EXISTS idx_gold_fact_returns_order
    ON gold.fact_returns (order_id);