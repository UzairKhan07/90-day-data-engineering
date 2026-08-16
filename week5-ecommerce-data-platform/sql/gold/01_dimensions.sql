-- Gold Layer - Dimensions

CREATE SCHEMA IF NOT EXISTS gold;

-- gold.dim_date
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key            DATE            PRIMARY KEY,
    year                INTEGER         NOT NULL,
    quarter             INTEGER         NOT NULL,
    month               INTEGER         NOT NULL,
    month_name          TEXT            NOT NULL,
    week_of_year        INTEGER,
    day_of_month        INTEGER         NOT NULL,
    day_of_week         INTEGER         NOT NULL,  
    day_name            TEXT            NOT NULL,
    is_weekend          BOOLEAN         NOT NULL
);

-- gold.dim_customers
CREATE TABLE IF NOT EXISTS gold.dim_customers (
    customer_sk             BIGSERIAL       PRIMARY KEY,   
    customer_id             TEXT            NOT NULL UNIQUE,
    customer_unique_id      TEXT,
    customer_city           TEXT,
    customer_state          TEXT,
    customer_zip_code_prefix TEXT,
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_dim_customers_state
    ON gold.dim_customers (customer_state);

-- gold.dim_products
CREATE TABLE IF NOT EXISTS gold.dim_products (
    product_sk              BIGSERIAL       PRIMARY KEY,
    product_id              TEXT            NOT NULL UNIQUE,
    product_category_name   TEXT,
    product_weight_g        NUMERIC(12, 2),
    product_name            TEXT,
    product_brand           TEXT,
    _loaded_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_dim_products_category
    ON gold.dim_products (product_category_name);