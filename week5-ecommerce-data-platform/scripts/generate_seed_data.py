# Generate realistic partner feed files for MinIO using REAL order_ids
# and product_ids from the downloaded Kaggle dataset.

from __future__ import annotations
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
import polars as pl

# Config
SEED = 42
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw" / "kaggle"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "seed_data"

# Realistic return rate
RETURN_RATE = 0.07

# Inventory snapshots
NUM_INVENTORY_SNAPSHOTS = 2

STATUSES = [
    "pending",
    "confirmed",
    "picked",
    "shipped",
    "in_transit",
    "out_for_delivery",
    "delivered",
    "delayed",
    "cancelled",
]

STATUS_WEIGHTS = [4, 6, 5, 15, 12, 10, 35, 5, 8]

RETURN_REASONS = [
    "defective",
    "wrong_item",
    "not_as_described",
    "changed_mind",
    "damaged_in_transit",
    "size_issue",
    "late_delivery",
    "other",
]

CARRIERS = ["UPS", "FedEx", "USPS", "DHL", "OnTrac", "Amazon Logistics"]

PRODUCT_CATEGORIES = [
    "electronics",
    "furniture",
    "fashion",
    "home_goods",
    "toys",
    "books",
    "auto",
]


def _load_orders() -> pl.DataFrame:
    path = RAW_DIR / "orders.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"orders.csv not found at {path}. "
            "Run scripts/download_kaggle_dataset.py first."
        )

    print(f"Loading orders from {path} ...")
    df = pl.read_csv(path, infer_schema_length=10000)
    print(f"  → {df.height:,} orders loaded")
    return df


def _load_products() -> pl.DataFrame:
    path = RAW_DIR / "products.csv"
    if not path.exists():
        raise FileNotFoundError(f"products.csv not found at {path}")

    print(f"Loading products from {path} ...")
    df = pl.read_csv(path, infer_schema_length=5000)
    print(f"  → {df.height:,} products loaded")
    return df


def _load_order_items() -> pl.DataFrame | None:
    path = RAW_DIR / "order_items.csv"
    if not path.exists():
        print("  order_items.csv not found – returns will use random product_ids")
        return None

    print(f"Loading order_items from {path} ...")
    df = pl.read_csv(path, infer_schema_length=10000)
    print(f"  → {df.height:,} order items loaded")
    return df


def generate_order_status(orders: pl.DataFrame) -> None:
    out_dir = OUTPUT_DIR / "order_status"
    out_dir.mkdir(parents=True, exist_ok=True)

    order_ids = orders["order_id"].to_list()
    total = len(order_ids)
    print(f"\nGenerating order status for {total:,} orders ...")

    # Split into multiple daily files to make it looks realistic
    NUM_FILES = 10
    chunk_size = (total + NUM_FILES - 1) // NUM_FILES
    base_date = datetime(2024, 6, 1)

    random.seed(SEED)

    for file_idx in range(NUM_FILES):
        start = file_idx * chunk_size
        end = min(start + chunk_size, total)
        chunk_ids = order_ids[start:end]

        if not chunk_ids:
            break

        file_date = base_date + timedelta(days=file_idx)
        filename = out_dir / f"order_status_{file_date.strftime('%Y-%m-%d')}.csv"

        rows = []
        for oid in chunk_ids:
            status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
            event_ts = file_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )

            has_tracking = status in (
                "shipped",
                "in_transit",
                "out_for_delivery",
                "delivered",
            )

            rows.append(
                {
                    "order_id": oid,
                    "status": status,
                    "status_timestamp": event_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "carrier": random.choice(CARRIERS) if has_tracking else "",
                    "tracking_number": (
                        f"1Z{random.randint(10**11, 10**12 - 1)}" if has_tracking else ""
                    ),
                    "warehouse_id": f"WH-{random.randint(1, 12):02d}",
                    "notes": "",
                    "source_system": "logistics_partner",
                    "file_date": file_date.strftime("%Y-%m-%d"),
                }
            )

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        print(f"  ✓ {filename.name}  ({len(rows):,} rows)")


def generate_returns(
    orders: pl.DataFrame, order_items: pl.DataFrame | None
) -> None:
    out_dir = OUTPUT_DIR / "returns"
    out_dir.mkdir(parents=True, exist_ok=True)

    order_ids = orders["order_id"].to_list()
    n_returns = int(len(order_ids) * RETURN_RATE)
    print(f"\nGenerating ~{n_returns:,} returns ({RETURN_RATE*100:.0f}% rate) ...")

    random.seed(SEED + 1)
    returned_order_ids = random.sample(order_ids, n_returns)

    order_to_products: dict[str, list[str]] = {}
    if order_items is not None:
        sample = order_items.select(["order_id", "product_id"]).unique()
        for row in sample.iter_rows(named=True):
            oid = str(row["order_id"])
            pid = str(row["product_id"])
            order_to_products.setdefault(oid, []).append(pid)

    NUM_FILES = 5
    chunk_size = (n_returns + NUM_FILES - 1) // NUM_FILES
    base_date = datetime(2024, 6, 5)

    for file_idx in range(NUM_FILES):
        start = file_idx * chunk_size
        end = min(start + chunk_size, n_returns)
        chunk = returned_order_ids[start:end]

        if not chunk:
            break

        file_date = base_date + timedelta(days=file_idx * 2)
        filename = out_dir / f"returns_{file_date.strftime('%Y-%m-%d')}.csv"

        rows = []
        for i, oid in enumerate(chunk):
            products = order_to_products.get(str(oid), [])
            if products:
                pid = random.choice(products)
            else:
                pid = f"PRD-{random.randint(10000, 12000)}"

            return_date = file_date - timedelta(days=random.randint(0, 7))
            refund_amount = round(random.uniform(9.99, 349.99), 2)

            rows.append(
                {
                    "return_id": f"RET-{file_date.strftime('%Y%m%d')}-{i+1:05d}",
                    "order_id": oid,
                    "product_id": pid,
                    "return_date": return_date.strftime("%Y-%m-%d"),
                    "return_reason": random.choice(RETURN_REASONS),
                    "refund_amount": refund_amount,
                    "refund_currency": "USD",
                    "return_status": random.choice(
                        ["requested", "approved", "received", "refunded", "rejected"]
                    ),
                    "quantity": random.randint(1, 3),
                    "source_system": "returns_portal",
                    "file_date": file_date.strftime("%Y-%m-%d"),
                }
            )

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        print(f"  ✓ {filename.name}  ({len(rows):,} rows)")


def generate_inventory(products: pl.DataFrame) -> None:
    out_dir = OUTPUT_DIR / "inventory"
    out_dir.mkdir(parents=True, exist_ok=True)

    product_ids = products["product_id"].to_list()
    print(f"\nGenerating inventory snapshots for {len(product_ids):,} products ...")

    random.seed(SEED + 2)
    base_date = datetime(2024, 6, 1)

    for snap_idx in range(NUM_INVENTORY_SNAPSHOTS):
        file_date = base_date + timedelta(days=snap_idx * 7)
        filename = out_dir / f"inventory_snapshot_{file_date.strftime('%Y-%m-%d')}.csv"

        rows = []
        for pid in product_ids:
            on_hand = random.randint(0, 600)
            reserved = random.randint(0, min(80, on_hand))
            available = on_hand - reserved

            rows.append(
                {
                    "product_id": pid,
                    "warehouse_id": f"WH-{random.randint(1, 12):02d}",
                    "snapshot_date": file_date.strftime("%Y-%m-%d"),
                    "quantity_on_hand": on_hand,
                    "quantity_reserved": reserved,
                    "quantity_available": available,
                    "reorder_point": random.randint(10, 100),
                    "category": random.choice(PRODUCT_CATEGORIES),
                    "is_stockout": available == 0,
                    "source_system": "wms",
                    "file_date": file_date.strftime("%Y-%m-%d"),
                }
            )

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        print(f"  ✓ {filename.name}  ({len(rows):,} rows)")


def main() -> None:
    print("=" * 60)
    print("Generating MinIO seed data from real Kaggle IDs")
    print("=" * 60)

    orders = _load_orders()
    products = _load_products()
    order_items = _load_order_items()

    for sub in ["order_status", "returns", "inventory"]:
        folder = OUTPUT_DIR / sub
        if folder.exists():
            for f in folder.glob("*.csv"):
                f.unlink()

    generate_order_status(orders)
    generate_returns(orders, order_items)
    generate_inventory(products)

    print("\n" + "=" * 60)
    print("✅ Seed data generation complete!")
    print(f"Files written to: {OUTPUT_DIR}")
    print("These will be uploaded to MinIO by the minio-init container")
    print("or by an Airflow task.")
    print("=" * 60)


if __name__ == "__main__":
    main()