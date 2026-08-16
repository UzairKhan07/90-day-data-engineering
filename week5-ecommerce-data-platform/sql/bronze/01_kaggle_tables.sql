-- Bronze Layer - Kaggle E-Commerce U.S. Dataset

CREATE SCHEMA IF NOT EXISTS bronze;

-- bronze.orders
CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id                        TEXT,
    customer_id                     TEXT,
    order_status                    TEXT,
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP,
    _source_system                  TEXT        NOT NULL DEFAULT 'kaggle',
    _loaded_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _batch_id                       TEXT,
    _row_hash                       TEXT,
    PRIMARY KEY (order_id)
);

CREATE INDEX IF NOT EXISTS idx_bronze_orders_customer
    ON bronze.orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_bronze_orders_status
    ON bronze.orders (order_status);

CREATE INDEX IF NOT EXISTS idx_bronze_orders_purchase_ts
    ON bronze.orders (order_purchase_timestamp);


-- bronze.order_items
CREATE TABLE IF NOT EXISTS bronze.order_items (
    order_id                TEXT,
    order_item_id           INTEGER,
    product_id              TEXT,
    seller_id               TEXT,
    shipping_limit_date     TIMESTAMP,
    price                   NUMERIC(12, 2),
    freight_value           NUMERIC(12, 2),
    discount_rate           NUMERIC(5, 4),
    _source_system          TEXT        NOT NULL DEFAULT 'kaggle',
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _batch_id               TEXT,
    _row_hash               TEXT,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE INDEX IF NOT EXISTS idx_bronze_order_items_product
    ON bronze.order_items (product_id);

CREATE INDEX IF NOT EXISTS idx_bronze_order_items_seller
    ON bronze.order_items (seller_id);


-- bronze.customers
CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id                 TEXT,
    customer_unique_id          TEXT,
    customer_zip_code_prefix    TEXT,
    customer_city               TEXT,
    customer_state              TEXT,
    _source_system              TEXT        NOT NULL DEFAULT 'kaggle',
    _loaded_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _batch_id                   TEXT,
    _row_hash                   TEXT,
    PRIMARY KEY (customer_id)
);

CREATE INDEX IF NOT EXISTS idx_bronze_customers_state
    ON bronze.customers (customer_state);


-- bronze.products
CREATE TABLE IF NOT EXISTS bronze.products (
    product_id                      TEXT,
    product_category_name           TEXT,
    product_name_lenght             INTEGER,
    product_description_lenght      INTEGER,
    product_photos_qty              INTEGER,
    product_weight_g                NUMERIC(12, 2),
    product_length_cm               NUMERIC(12, 2),
    product_height_cm               NUMERIC(12, 2),
    product_width_cm                NUMERIC(12, 2),
    product_name                    TEXT,
    product_brand                   TEXT,
    cost                            NUMERIC(12, 2),
    price                           NUMERIC(12, 2),
    _source_system                  TEXT        NOT NULL DEFAULT 'kaggle',
    _loaded_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _batch_id                       TEXT,
    _row_hash                       TEXT,
    PRIMARY KEY (product_id)
);

CREATE INDEX IF NOT EXISTS idx_bronze_products_category
    ON bronze.products (product_category_name);


-- bronze.sellers
CREATE TABLE IF NOT EXISTS bronze.sellers (
    seller_id                   TEXT,
    seller_zip_code_prefix      TEXT,
    seller_city                 TEXT,
    seller_state                TEXT,
    _source_system              TEXT        NOT NULL DEFAULT 'kaggle',
    _loaded_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _batch_id                   TEXT,
    _row_hash                   TEXT,

    PRIMARY KEY (seller_id)
);


-- bronze.order_payments
CREATE TABLE IF NOT EXISTS bronze.order_payments (
    order_id                TEXT,
    payment_sequential      INTEGER,
    payment_type            TEXT,
    payment_installments    INTEGER,
    payment_value           NUMERIC(12, 2),
    _source_system          TEXT        NOT NULL DEFAULT 'kaggle',
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _batch_id               TEXT,
    _row_hash               TEXT,
    PRIMARY KEY (order_id, payment_sequential)
);


-- bronze.order_reviews
CREATE TABLE IF NOT EXISTS bronze.order_reviews (
    review_id                   TEXT,
    order_id                    TEXT,
    review_score                INTEGER,
    review_comment_title        TEXT,
    review_comment_message      TEXT,
    review_creation_date        TIMESTAMP,
    review_answer_timestamp     TIMESTAMP,
    _source_system              TEXT        NOT NULL DEFAULT 'kaggle',
    _loaded_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _batch_id                   TEXT,
    _row_hash                   TEXT,
    PRIMARY KEY (review_id)
);

CREATE INDEX IF NOT EXISTS idx_bronze_reviews_order
    ON bronze.order_reviews (order_id);