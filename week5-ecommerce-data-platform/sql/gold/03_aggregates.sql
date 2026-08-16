-- Gold Layer - Aggregates / KPI tables (dashboard-friendly)

CREATE SCHEMA IF NOT EXISTS gold;

-- gold.daily_sales_summary
CREATE TABLE IF NOT EXISTS gold.daily_sales_summary (
    sales_date                  DATE            PRIMARY KEY,
    total_orders                INTEGER,
    delivered_orders            INTEGER,
    cancelled_orders            INTEGER,
    delayed_orders              INTEGER,
    total_items                 INTEGER,
    gross_revenue               NUMERIC(16, 2),
    net_revenue                 NUMERIC(16, 2),
    total_freight               NUMERIC(16, 2),
    avg_order_value             NUMERIC(12, 2),
    total_returns               INTEGER,
    total_refund_amount         NUMERIC(16, 2),
    return_rate                 NUMERIC(8, 4),   
    _loaded_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);


-- gold.product_performance
CREATE TABLE IF NOT EXISTS gold.product_performance (
    product_id                  TEXT            PRIMARY KEY,
    product_category_name       TEXT,
    times_ordered               INTEGER,        
    total_quantity_proxy        INTEGER,        
    gross_revenue               NUMERIC(16, 2),
    net_revenue                 NUMERIC(16, 2),
    times_returned              INTEGER,
    total_refund_amount         NUMERIC(16, 2),
    return_rate                 NUMERIC(8, 4),
    _loaded_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_product_perf_category
    ON gold.product_performance (product_category_name);


-- gold.return_rate_by_reason
CREATE TABLE IF NOT EXISTS gold.return_rate_by_reason (
    return_reason               TEXT            PRIMARY KEY,
    return_count                INTEGER,
    total_refund_amount         NUMERIC(16, 2),
    pct_of_all_returns          NUMERIC(8, 4),
    _loaded_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);


-- gold.delivery_performance
CREATE TABLE IF NOT EXISTS gold.delivery_performance (
    snapshot_date               DATE            PRIMARY KEY,
    total_orders                INTEGER,
    delivered_orders            INTEGER,
    cancelled_orders            INTEGER,
    delayed_orders              INTEGER,
    delivery_rate               NUMERIC(8, 4),
    cancellation_rate           NUMERIC(8, 4),
    delay_rate                  NUMERIC(8, 4),
    avg_delay_days              NUMERIC(8, 2),
    _loaded_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);