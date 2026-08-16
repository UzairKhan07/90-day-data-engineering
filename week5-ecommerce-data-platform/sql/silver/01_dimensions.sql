-- Silver Layer - Dimensions

CREATE SCHEMA IF NOT EXISTS silver;

-- silver.customers
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id                 TEXT            PRIMARY KEY,
    customer_unique_id          TEXT,
    customer_zip_code_prefix    TEXT,
    customer_city               TEXT,
    customer_state              TEXT,
    _source_system              TEXT            NOT NULL DEFAULT 'kaggle',
    _loaded_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _batch_id                   TEXT,
    _row_hash                   TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_customers_state
    ON silver.customers (customer_state);

CREATE INDEX IF NOT EXISTS idx_silver_customers_unique_id
    ON silver.customers (customer_unique_id);


-- silver.products
CREATE TABLE IF NOT EXISTS silver.products (
    product_id                  TEXT            PRIMARY KEY,
    product_category_name       TEXT,
    product_name_lenght         INTEGER,
    product_description_lenght  INTEGER,
    product_photos_qty          INTEGER,
    product_weight_g            NUMERIC(12, 2),
    product_length_cm           NUMERIC(12, 2),
    product_height_cm           NUMERIC(12, 2),
    product_width_cm            NUMERIC(12, 2),
    product_name                TEXT,
    product_brand               TEXT,
    cost                        NUMERIC(12, 2),
    price                       NUMERIC(12, 2),
    _source_system              TEXT            NOT NULL DEFAULT 'kaggle',
    _loaded_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _batch_id                   TEXT,
    _row_hash                   TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_products_category
    ON silver.products (product_category_name);


-- silver.sellers
CREATE TABLE IF NOT EXISTS silver.sellers (
    seller_id                   TEXT            PRIMARY KEY,
    seller_zip_code_prefix      TEXT,
    seller_city                 TEXT,
    seller_state                TEXT,
    _source_system              TEXT            NOT NULL DEFAULT 'kaggle',
    _loaded_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _batch_id                   TEXT,
    _row_hash                   TEXT
);