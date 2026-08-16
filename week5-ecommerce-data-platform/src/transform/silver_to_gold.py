from __future__ import annotations
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from src.utils.db import get_connection

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))


def _run(sql: str, params: tuple | None = None) -> int:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount


# Dimensions
def load_dim_date() -> int:
    print("\n→ gold.dim_date")
    sql = """
        INSERT INTO gold.dim_date (
            date_key, year, quarter, month, month_name,
            week_of_year, day_of_month, day_of_week, day_name, is_weekend
        )
        SELECT DISTINCT
            d::date AS date_key,
            EXTRACT(YEAR FROM d)::int,
            EXTRACT(QUARTER FROM d)::int,
            EXTRACT(MONTH FROM d)::int,
            TO_CHAR(d, 'Month'),
            EXTRACT(WEEK FROM d)::int,
            EXTRACT(DAY FROM d)::int,
            EXTRACT(ISODOW FROM d)::int,
            TO_CHAR(d, 'Day'),
            EXTRACT(ISODOW FROM d)::int IN (6, 7)
        FROM (
            SELECT DISTINCT order_purchase_timestamp::date AS d
            FROM silver.orders
            WHERE order_purchase_timestamp IS NOT NULL
        ) s
        ON CONFLICT (date_key) DO NOTHING
    """
    count = _run(sql)
    print(f"  ✓ dim_date (rowcount≈{count})")
    return count


def load_dim_customers() -> int:
    print("\n→ gold.dim_customers")
    sql = """
        INSERT INTO gold.dim_customers (
            customer_id, customer_unique_id, customer_city,
            customer_state, customer_zip_code_prefix
        )
        SELECT
            customer_id,
            customer_unique_id,
            customer_city,
            customer_state,
            customer_zip_code_prefix
        FROM silver.customers
        ON CONFLICT (customer_id) DO NOTHING
    """
    count = _run(sql)
    print(f"  ✓ dim_customers (rowcount≈{count})")
    return count


def load_dim_products() -> int:
    print("\n→ gold.dim_products")

    sql_insert = """
        INSERT INTO gold.dim_products (
            product_id, product_category_name, product_weight_g,
            product_name, product_brand
        )
        SELECT
            product_id,
            product_category_name,
            product_weight_g,
            product_name,
            product_brand
        FROM silver.products
        ON CONFLICT (product_id) DO NOTHING
    """
    count = _run(sql_insert)
    print(f"  ✓ dim_products loaded (rowcount≈{count})")

    # Assign unique readable names by category
    sql_names = """
        WITH ranked AS (
            SELECT
                product_id,
                COALESCE(product_category_name, 'product') AS category,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(product_category_name, 'product')
                    ORDER BY product_id
                ) AS rn
            FROM gold.dim_products
        ),
        pools AS (
            SELECT * FROM (VALUES
                ('electronics', ARRAY[
                    'Laptop Pro','Tablet Air','Smartphone X','Wireless Earbuds','4K Monitor',
                    'Gaming Mouse','Mechanical Keyboard','Bluetooth Speaker','USB-C Hub',
                    'Webcam HD','Power Bank','Smartwatch','External SSD','WiFi Router',
                    'Graphics Card','Portable Charger','Noise Cancelling Headphones','Smart TV Stick'
                ]),
                ('fashion', ARRAY[
                    'Classic T-Shirt','Slim Jeans','Running Sneakers','Leather Jacket','Summer Dress',
                    'Hoodie','Canvas Backpack','Wool Scarf','Baseball Cap','Casual Shirt',
                    'Ankle Boots','Denim Jacket','Chino Pants','Knit Sweater','Raincoat',
                    'Sports Shorts','Leather Belt','Sunglasses'
                ]),
                ('home_goods', ARRAY[
                    'Table Lamp','Ceramic Vase','Throw Pillow','Wall Clock','Coffee Maker',
                    'Nonstick Pan','Storage Bin','Area Rug','Desk Organizer','Scented Candle',
                    'Cutlery Set','Bedsheet Set','Kitchen Scale','Laundry Basket','Soap Dispenser',
                    'Photo Frame','Trash Can','Water Bottle Set'
                ]),
                ('furniture', ARRAY[
                    'Office Chair','Wooden Desk','Bookshelf','Sofa Set','Dining Table',
                    'Bed Frame','TV Stand','Wardrobe','Side Table','Recliner',
                    'Shoe Rack','Coffee Table','Nightstand','Bar Stool','Filing Cabinet',
                    'Accent Chair','Console Table','Ottoman'
                ]),
                ('toys', ARRAY[
                    'Building Blocks','Remote Car','Puzzle Set','Action Figure','Board Game',
                    'Plush Toy','LEGO Kit','RC Helicopter','Art Set','Yo-Yo Pro',
                    'Science Kit','Doll House','Train Set','Slime Kit','Stuffed Animal',
                    'Card Game','Robot Toy','Water Gun'
                ]),
                ('books', ARRAY[
                    'Mystery Novel','Data Engineering Guide','Sci-Fi Classic','Cookbook Essentials',
                    'History of Tech','Self-Help Handbook','Fantasy Epic','Business Strategy',
                    'Travel Diary','Poetry Collection','Startup Playbook','AI Handbook',
                    'Finance Basics','Biography Collection','Thriller Series','Design Principles',
                    'Health & Fitness','Short Stories'
                ]),
                ('auto', ARRAY[
                    'Car Phone Mount','Dash Cam','Tire Inflator','Seat Cover','LED Headlights',
                    'Oil Filter','Car Vacuum','Jump Starter','Floor Mats','Steering Cover',
                    'Air Freshener Pack','Tool Kit','Wiper Blades','Car Charger','Trunk Organizer',
                    'Tire Gauge','Cleaning Kit','Sun Shade'
                ])
            ) AS t(category, names)
        ),
        named AS (
            SELECT
                r.product_id,
                p.names[1 + ((r.rn - 1) % array_length(p.names, 1))]
                || ' #' || LPAD(r.rn::text, 3, '0') AS product_name
            FROM ranked r
            JOIN pools p ON p.category = r.category
        )
        UPDATE gold.dim_products d
        SET product_name = n.product_name
        FROM named n
        WHERE d.product_id = n.product_id
    """
    named_count = _run(sql_names)
    print(f"  ✓ product names assigned (rowcount≈{named_count})")
    return count

# Facts
def load_fact_order_items() -> int:
    print("\n→ gold.fact_order_items")
    sql = """
        INSERT INTO gold.fact_order_items (
            order_id, order_item_id, product_id, seller_id,
            order_purchase_date, price, freight_value,
            discount_rate, line_total
        )
        SELECT
            oi.order_id,
            oi.order_item_id,
            oi.product_id,
            oi.seller_id,
            o.order_purchase_timestamp::date,
            oi.price,
            oi.freight_value,
            oi.discount_rate,
            oi.line_total
        FROM silver.order_items oi
        JOIN silver.orders o ON o.order_id = oi.order_id
        ON CONFLICT (order_id, order_item_id) DO NOTHING
    """
    count = _run(sql)
    print(f"  ✓ fact_order_items (rowcount≈{count})")
    return count


def load_fact_orders() -> int:
    print("\n→ gold.fact_orders")
    sql = """
        INSERT INTO gold.fact_orders (
            order_id, customer_id, order_purchase_date, order_status,
            latest_logistics_status, carrier, warehouse_id,
            is_delivered, is_cancelled, is_delayed, delivery_delay_days,
            order_item_count, order_gross_value, order_freight_value, order_net_value,
            has_return, return_count, total_refund_amount
        )
        SELECT
            o.order_id,
            o.customer_id,
            o.order_purchase_timestamp::date,
            o.order_status,
            o.latest_logistics_status,
            o.carrier,
            o.warehouse_id,
            o.is_delivered,
            o.is_cancelled,
            o.is_delayed,
            o.delivery_delay_days,

            COALESCE(agg.item_count, 0),
            COALESCE(agg.gross_value, 0),
            COALESCE(agg.freight_value, 0),
            COALESCE(agg.net_value, 0),

            COALESCE(ret.has_return, FALSE),
            COALESCE(ret.return_count, 0),
            COALESCE(ret.total_refund, 0)
        FROM silver.orders o
        LEFT JOIN (
            SELECT
                order_id,
                COUNT(*)                    AS item_count,
                SUM(price)                  AS gross_value,
                SUM(freight_value)          AS freight_value,
                SUM(line_total)             AS net_value
            FROM silver.order_items
            GROUP BY order_id
        ) agg ON agg.order_id = o.order_id
        LEFT JOIN (
            SELECT
                order_id,
                TRUE                        AS has_return,
                COUNT(*)                    AS return_count,
                SUM(refund_amount)          AS total_refund
            FROM silver.returns
            GROUP BY order_id
        ) ret ON ret.order_id = o.order_id
        ON CONFLICT (order_id) DO NOTHING
    """
    count = _run(sql)
    print(f"  ✓ fact_orders (rowcount≈{count})")
    return count


def load_fact_returns() -> int:
    print("\n→ gold.fact_returns")
    sql = """
        INSERT INTO gold.fact_returns (
            return_id, order_id, product_id, return_date,
            return_reason, return_status, quantity,
            refund_amount, refund_currency
        )
        SELECT
            return_id,
            order_id,
            product_id,
            return_date,
            return_reason,
            return_status,
            quantity,
            refund_amount,
            refund_currency
        FROM silver.returns
        ON CONFLICT (return_id) DO NOTHING
    """
    count = _run(sql)
    print(f"  ✓ fact_returns (rowcount≈{count})")
    return count


# Aggregates / KPIs
def load_daily_sales_summary() -> int:
    print("\n→ gold.daily_sales_summary")
    _run("TRUNCATE gold.daily_sales_summary")

    sql = """
        INSERT INTO gold.daily_sales_summary (
            sales_date, total_orders, delivered_orders, cancelled_orders, delayed_orders,
            total_items, gross_revenue, net_revenue, total_freight,
            avg_order_value, total_returns, total_refund_amount, return_rate
        )
        SELECT
            f.order_purchase_date                           AS sales_date,
            COUNT(*)                                        AS total_orders,
            COUNT(*) FILTER (WHERE f.is_delivered)          AS delivered_orders,
            COUNT(*) FILTER (WHERE f.is_cancelled)          AS cancelled_orders,
            COUNT(*) FILTER (WHERE f.is_delayed)            AS delayed_orders,

            COALESCE(SUM(f.order_item_count), 0)            AS total_items,
            COALESCE(SUM(f.order_gross_value), 0)           AS gross_revenue,
            COALESCE(SUM(f.order_net_value), 0)             AS net_revenue,
            COALESCE(SUM(f.order_freight_value), 0)         AS total_freight,

            CASE WHEN COUNT(*) > 0
                 THEN ROUND(SUM(f.order_net_value) / COUNT(*), 2)
                 ELSE 0
            END                                             AS avg_order_value,

            COALESCE(SUM(f.return_count), 0)                AS total_returns,
            COALESCE(SUM(f.total_refund_amount), 0)         AS total_refund_amount,

            CASE WHEN COUNT(*) > 0
                 THEN ROUND(SUM(f.return_count)::numeric / COUNT(*), 4)
                 ELSE 0
            END                                             AS return_rate
        FROM gold.fact_orders f
        WHERE f.order_purchase_date IS NOT NULL
        GROUP BY f.order_purchase_date
    """
    count = _run(sql)
    print(f"  ✓ daily_sales_summary (rowcount≈{count})")
    return count


def load_product_performance() -> int:
    print("\n→ gold.product_performance")
    _run("TRUNCATE gold.product_performance")

    sql = """
        INSERT INTO gold.product_performance (
            product_id, product_category_name,
            times_ordered, total_quantity_proxy,
            gross_revenue, net_revenue,
            times_returned, total_refund_amount, return_rate
        )
        SELECT
            p.product_id,
            p.product_category_name,
            COALESCE(ord.times_ordered, 0),
            COALESCE(ord.times_ordered, 0),
            COALESCE(ord.gross_revenue, 0),
            COALESCE(ord.net_revenue, 0),
            COALESCE(ret.times_returned, 0),
            COALESCE(ret.total_refund, 0),
            CASE WHEN COALESCE(ord.times_ordered, 0) > 0
                 THEN ROUND(COALESCE(ret.times_returned, 0)::numeric / ord.times_ordered, 4)
                 ELSE 0
            END
        FROM gold.dim_products p
        LEFT JOIN (
            SELECT
                product_id,
                COUNT(*)            AS times_ordered,
                SUM(price)          AS gross_revenue,
                SUM(line_total)     AS net_revenue
            FROM gold.fact_order_items
            GROUP BY product_id
        ) ord ON ord.product_id = p.product_id
        LEFT JOIN (
            SELECT
                product_id,
                COUNT(*)            AS times_returned,
                SUM(refund_amount)  AS total_refund
            FROM gold.fact_returns
            GROUP BY product_id
        ) ret ON ret.product_id = p.product_id
    """
    count = _run(sql)
    print(f"  ✓ product_performance (rowcount≈{count})")
    return count


def load_return_rate_by_reason() -> int:
    print("\n→ gold.return_rate_by_reason")
    _run("TRUNCATE gold.return_rate_by_reason")

    sql = """
        INSERT INTO gold.return_rate_by_reason (
            return_reason, return_count, total_refund_amount, pct_of_all_returns
        )
        SELECT
            return_reason,
            COUNT(*)                                        AS return_count,
            COALESCE(SUM(refund_amount), 0)                 AS total_refund_amount,
            ROUND(
                COUNT(*)::numeric / NULLIF(SUM(COUNT(*)) OVER (), 0),
                4
            )                                               AS pct_of_all_returns
        FROM gold.fact_returns
        WHERE return_reason IS NOT NULL
        GROUP BY return_reason
    """
    count = _run(sql)
    print(f"  ✓ return_rate_by_reason (rowcount≈{count})")
    return count


def load_delivery_performance() -> int:
    print("\n→ gold.delivery_performance")
    _run("TRUNCATE gold.delivery_performance")

    sql = """
        INSERT INTO gold.delivery_performance (
            snapshot_date, total_orders, delivered_orders, cancelled_orders, delayed_orders,
            delivery_rate, cancellation_rate, delay_rate, avg_delay_days
        )
        SELECT
            CURRENT_DATE,
            COUNT(*),
            COUNT(*) FILTER (WHERE is_delivered),
            COUNT(*) FILTER (WHERE is_cancelled),
            COUNT(*) FILTER (WHERE is_delayed),
            ROUND(COUNT(*) FILTER (WHERE is_delivered)::numeric / NULLIF(COUNT(*), 0), 4),
            ROUND(COUNT(*) FILTER (WHERE is_cancelled)::numeric / NULLIF(COUNT(*), 0), 4),
            ROUND(COUNT(*) FILTER (WHERE is_delayed)::numeric / NULLIF(COUNT(*), 0), 4),
            ROUND(AVG(delivery_delay_days) FILTER (WHERE is_delayed), 2)
        FROM gold.fact_orders
    """
    count = _run(sql)
    print(f"  ✓ delivery_performance (rowcount≈{count})")
    return count


# Orchestrator
def run_silver_to_gold() -> dict:
    print("=" * 60)
    print("Silver → Gold transformation")
    print("=" * 60)

    results = {}
    results["dim_date"] = load_dim_date()
    results["dim_customers"] = load_dim_customers()
    results["dim_products"] = load_dim_products()
    results["fact_order_items"] = load_fact_order_items()
    results["fact_orders"] = load_fact_orders()
    results["fact_returns"] = load_fact_returns()
    results["daily_sales_summary"] = load_daily_sales_summary()
    results["product_performance"] = load_product_performance()
    results["return_rate_by_reason"] = load_return_rate_by_reason()
    results["delivery_performance"] = load_delivery_performance()

    print("\n" + "=" * 60)
    print("Gold load summary")
    for k, v in results.items():
        print(f"  {k}: {v}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    run_silver_to_gold()