from __future__ import annotations
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
import polars as pl
from psycopg2.extras import execute_values
from src.utils.db import get_connection
from src.utils.hashing import row_hash

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "kaggle"

# Config
TABLE_CONFIGS = [
    {
        "csv": "orders.csv",
        "table": "bronze.orders",
        "columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "pk": ["order_id"],
        "timestamp_cols": [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    },
    {
        "csv": "order_items.csv",
        "table": "bronze.order_items",
        "columns": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
            "discount_rate",
        ],
        "pk": ["order_id", "order_item_id"],
        "timestamp_cols": ["shipping_limit_date"],
    },
    {
        "csv": "customers.csv",
        "table": "bronze.customers",
        "columns": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
        "pk": ["customer_id"],
        "timestamp_cols": [],
    },
    {
        "csv": "products.csv",
        "table": "bronze.products",
        "columns": [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
        "pk": ["product_id"],
        "timestamp_cols": [],
    },
]


def _read_csv(path: Path, expected_cols: list[str]) -> pl.DataFrame:
    df = pl.read_csv(path, infer_schema_length=10000, ignore_errors=True)
    available = [c for c in expected_cols if c in df.columns]
    missing = [c for c in expected_cols if c not in df.columns]

    if missing:
        print(f"  ⚠ Missing columns in {path.name}: {missing}")

    return df.select(available)


def _prepare_rows(
    df: pl.DataFrame,
    business_cols: list[str],
    batch_id: str,
) -> list[tuple]:
    rows = []
    now = datetime.utcnow()

    cols = [c for c in business_cols if c in df.columns]

    for record in df.select(cols).iter_rows(named=True):
        values = [record.get(c) for c in cols]
        h = row_hash(values)

        full_row = tuple(values) + (
            "kaggle",       # source_system
            now,            # loaded_at
            batch_id,       # batch_id
            h,              # row_hash
        )
        rows.append(full_row)

    return rows


def load_table(
    csv_name: str,
    table: str,
    columns: list[str],
    pk: list[str],
    batch_id: str,
) -> dict:
    path = RAW_DIR / csv_name
    if not path.exists():
        print(f"  ✗ File not found: {path}")
        return {"table": table, "status": "skipped", "reason": "file_not_found"}

    print(f"\n→ Loading {csv_name} → {table}")
    df = _read_csv(path, columns)
    print(f"  Rows in file: {df.height:,}")

    if df.height == 0:
        return {"table": table, "status": "skipped", "reason": "empty_file"}

    insert_cols = [c for c in columns if c in df.columns] + [
        "_source_system",
        "_loaded_at",
        "_batch_id",
        "_row_hash",
    ]

    rows = _prepare_rows(df, columns, batch_id)
    print(f"  Prepared rows: {len(rows):,}")

    conflict_target = ", ".join(pk)
    col_list = ", ".join(insert_cols)

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES %s
        ON CONFLICT ({conflict_target}) DO NOTHING
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)

    stats = {
        "table": table,
        "status": "success",
        "rows_read": df.height,
        "rows_attempted": len(rows),
        "batch_id": batch_id,
    }
    print(f"  ✓ Done → {table}")
    return stats


def load_all_kaggle_tables(batch_id: str | None = None) -> list[dict]:
    if batch_id is None:
        batch_id = f"kaggle_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    print("=" * 60)
    print("Kaggle → Bronze loader")
    print(f"Batch ID : {batch_id}")
    print(f"Source   : {RAW_DIR}")
    print("=" * 60)

    if not RAW_DIR.exists():
        raise FileNotFoundError(
            f"Raw Kaggle directory not found: {RAW_DIR}\n"
            "Run scripts/download_kaggle_dataset.py first."
        )

    results = []
    for cfg in TABLE_CONFIGS:
        result = load_table(
            csv_name=cfg["csv"],
            table=cfg["table"],
            columns=cfg["columns"],
            pk=cfg["pk"],
            batch_id=batch_id,
        )
        results.append(result)

    print("\n" + "=" * 60)
    print("Summary")
    for r in results:
        print(f"  {r['table']}: {r['status']}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    load_all_kaggle_tables()