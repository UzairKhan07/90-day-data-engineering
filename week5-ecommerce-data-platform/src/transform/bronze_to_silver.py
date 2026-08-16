from __future__ import annotations
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from psycopg2.extras import execute_values
from src.utils.db import get_connection
from src.utils.hashing import row_hash

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _run_sql(sql: str, params: tuple | None = None) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


def _fetch_all(sql: str, params: tuple | None = None):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


# 1. Dimensions
def transform_customers(batch_id: str) -> int:
    print("\n→ silver.customers")
    sql = """
        INSERT INTO silver.customers (
            customer_id, customer_unique_id, customer_zip_code_prefix,
            customer_city, customer_state,
            _source_system, _loaded_at, _batch_id, _row_hash
        )
        SELECT
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            INITCAP(TRIM(customer_city)),
            UPPER(TRIM(customer_state)),
            'kaggle',
            NOW(),
            %s,
            md5(CONCAT_WS('||',
                COALESCE(customer_id, 'NULL'),
                COALESCE(customer_unique_id, 'NULL'),
                COALESCE(customer_zip_code_prefix, 'NULL'),
                COALESCE(customer_city, 'NULL'),
                COALESCE(customer_state, 'NULL')
            ))
        FROM bronze.customers
        ON CONFLICT (customer_id) DO NOTHING
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (batch_id,))
            count = cur.rowcount
    print(f"  ✓ customers processed (rowcount≈{count})")
    return count


def transform_products(batch_id: str) -> int:
    print("\n→ silver.products")
    sql = """
        INSERT INTO silver.products (
            product_id, product_category_name,
            product_name_lenght, product_description_lenght, product_photos_qty,
            product_weight_g, product_length_cm, product_height_cm, product_width_cm,
            product_name, product_brand, cost, price,
            _source_system, _loaded_at, _batch_id, _row_hash
        )
        SELECT
            product_id,
            LOWER(TRIM(product_category_name)),
            product_name_lenght,
            product_description_lenght,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm,
            product_name,
            product_brand,
            cost,
            price,
            'kaggle',
            NOW(),
            %s,
            md5(CONCAT_WS('||',
                COALESCE(product_id, 'NULL'),
                COALESCE(product_category_name, 'NULL')
            ))
        FROM bronze.products
        ON CONFLICT (product_id) DO NOTHING
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (batch_id,))
            count = cur.rowcount
    print(f"  ✓ products processed (rowcount≈{count})")
    return count


# 2. Facts
def transform_orders(batch_id: str) -> int:
    print("\n→ silver.orders (with latest logistics status)")

    sql = """
        INSERT INTO silver.orders (
            order_id, customer_id, order_status,
            order_purchase_timestamp, order_approved_at,
            order_delivered_carrier_date, order_delivered_customer_date,
            order_estimated_delivery_date,
            latest_logistics_status, latest_status_timestamp,
            carrier, tracking_number, warehouse_id,
            is_delivered, is_cancelled, is_delayed, delivery_delay_days,
            _source_system, _loaded_at, _batch_id, _row_hash
        )
        SELECT
            o.order_id,
            o.customer_id,
            LOWER(TRIM(o.order_status)),
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,

            s.status                          AS latest_logistics_status,
            s.status_timestamp                AS latest_status_timestamp,
            NULLIF(TRIM(s.carrier), '')       AS carrier,
            NULLIF(TRIM(s.tracking_number), '') AS tracking_number,
            s.warehouse_id,

            -- derived flags
            CASE
                WHEN LOWER(TRIM(o.order_status)) = 'delivered'
                  OR LOWER(COALESCE(s.status, '')) = 'delivered'
                THEN TRUE ELSE FALSE
            END AS is_delivered,

            CASE
                WHEN LOWER(TRIM(o.order_status)) IN ('canceled', 'cancelled')
                  OR LOWER(COALESCE(s.status, '')) = 'cancelled'
                THEN TRUE ELSE FALSE
            END AS is_cancelled,

            CASE
                WHEN o.order_delivered_customer_date IS NOT NULL
                 AND o.order_estimated_delivery_date IS NOT NULL
                 AND o.order_delivered_customer_date::date
                     > o.order_estimated_delivery_date::date
                THEN TRUE ELSE FALSE
            END AS is_delayed,

            CASE
                WHEN o.order_delivered_customer_date IS NOT NULL
                 AND o.order_estimated_delivery_date IS NOT NULL
                THEN (o.order_delivered_customer_date::date
                      - o.order_estimated_delivery_date::date)
                ELSE NULL
            END AS delivery_delay_days,

            'kaggle+minio',
            NOW(),
            %s,
            md5(CONCAT_WS('||',
                COALESCE(o.order_id, 'NULL'),
                COALESCE(o.customer_id, 'NULL'),
                COALESCE(o.order_status, 'NULL')
            ))
        FROM bronze.orders o
        LEFT JOIN LATERAL (
            SELECT status, status_timestamp, carrier, tracking_number, warehouse_id
            FROM bronze.order_status os
            WHERE os.order_id = o.order_id
            ORDER BY os.status_timestamp DESC NULLS LAST
            LIMIT 1
        ) s ON TRUE
        ON CONFLICT (order_id) DO NOTHING
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (batch_id,))
            count = cur.rowcount
    print(f"  ✓ orders processed (rowcount≈{count})")
    return count


def transform_order_items(batch_id: str) -> int:
    print("\n→ silver.order_items")
    sql = """
        INSERT INTO silver.order_items (
            order_id, order_item_id, product_id, seller_id,
            shipping_limit_date, price, freight_value, discount_rate, line_total,
            _source_system, _loaded_at, _batch_id, _row_hash
        )
        SELECT
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value,
            COALESCE(discount_rate, 0),
            ROUND(
                price * (1 - COALESCE(discount_rate, 0))
            , 2) AS line_total,
            'kaggle',
            NOW(),
            %s,
            md5(CONCAT_WS('||',
                COALESCE(order_id, 'NULL'),
                COALESCE(order_item_id::text, 'NULL'),
                COALESCE(product_id, 'NULL')
            ))
        FROM bronze.order_items
        ON CONFLICT (order_id, order_item_id) DO NOTHING
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (batch_id,))
            count = cur.rowcount
    print(f"  ✓ order_items processed (rowcount≈{count})")
    return count


def transform_returns(batch_id: str) -> int:
    print("\n→ silver.returns")
    sql = """
        INSERT INTO silver.returns (
            return_id, order_id, product_id, return_date,
            return_reason, refund_amount, refund_currency,
            return_status, quantity,
            _source_system, _loaded_at, _batch_id, _row_hash
        )
        SELECT
            return_id,
            order_id,
            product_id,
            return_date,
            LOWER(TRIM(return_reason)),
            refund_amount,
            UPPER(TRIM(refund_currency)),
            LOWER(TRIM(return_status)),
            quantity,
            'minio',
            NOW(),
            %s,
            md5(CONCAT_WS('||',
                COALESCE(return_id, 'NULL'),
                COALESCE(order_id, 'NULL'),
                COALESCE(product_id, 'NULL')
            ))
        FROM bronze.returns
        ON CONFLICT (return_id) DO NOTHING
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (batch_id,))
            count = cur.rowcount
    print(f"  ✓ returns processed (rowcount≈{count})")
    return count


def transform_inventory(batch_id: str) -> int:
    print("\n→ silver.inventory")
    sql = """
        INSERT INTO silver.inventory (
            product_id, warehouse_id, snapshot_date,
            quantity_on_hand, quantity_reserved, quantity_available,
            reorder_point, category, is_stockout,
            _source_system, _loaded_at, _batch_id, _row_hash
        )
        SELECT
            product_id,
            warehouse_id,
            snapshot_date,
            quantity_on_hand,
            quantity_reserved,
            quantity_available,
            reorder_point,
            LOWER(TRIM(category)),
            is_stockout,
            'minio',
            NOW(),
            %s,
            md5(CONCAT_WS('||',
                COALESCE(product_id, 'NULL'),
                COALESCE(warehouse_id, 'NULL'),
                COALESCE(snapshot_date::text, 'NULL')
            ))
        FROM bronze.inventory_snapshots
        ON CONFLICT (product_id, warehouse_id, snapshot_date) DO NOTHING
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (batch_id,))
            count = cur.rowcount
    print(f"  ✓ inventory processed (rowcount≈{count})")
    return count


def transform_currency_rates(batch_id: str) -> int:
    print("\n→ silver.currency_rates")
    sql = """
        INSERT INTO silver.currency_rates (
            rate_date, base_currency, target_currency, rate,
            _source_system, _loaded_at, _batch_id, _row_hash
        )
        SELECT
            rate_date,
            UPPER(TRIM(base_currency)),
            UPPER(TRIM(target_currency)),
            rate,
            'frankfurter',
            NOW(),
            %s,
            md5(CONCAT_WS('||',
                COALESCE(rate_date::text, 'NULL'),
                COALESCE(base_currency, 'NULL'),
                COALESCE(target_currency, 'NULL'),
                COALESCE(rate::text, 'NULL')
            ))
        FROM bronze.currency_rates
        ON CONFLICT (rate_date, base_currency, target_currency) DO NOTHING
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (batch_id,))
            count = cur.rowcount
    print(f"  ✓ currency_rates processed (rowcount≈{count})")
    return count


# Orchestrator
def run_bronze_to_silver(batch_id: str | None = None) -> dict:
    if batch_id is None:
        batch_id = (
            f"silver_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_"
            f"{uuid.uuid4().hex[:8]}"
        )

    print("=" * 60)
    print("Bronze → Silver transformation")
    print(f"Batch ID : {batch_id}")
    print("=" * 60)

    results = {}
    results["customers"] = transform_customers(batch_id)
    results["products"] = transform_products(batch_id)
    results["orders"] = transform_orders(batch_id)
    results["order_items"] = transform_order_items(batch_id)
    results["returns"] = transform_returns(batch_id)
    results["inventory"] = transform_inventory(batch_id)
    results["currency_rates"] = transform_currency_rates(batch_id)

    print("\n" + "=" * 60)
    print("Silver load summary")
    for k, v in results.items():
        print(f"  {k}: {v}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    run_bronze_to_silver()