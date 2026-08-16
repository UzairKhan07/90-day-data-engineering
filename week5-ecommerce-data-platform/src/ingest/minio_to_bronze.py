from __future__ import annotations
import hashlib
import io
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
import polars as pl
from minio import Minio
from minio.error import S3Error
from psycopg2.extras import execute_values
from src.utils.db import get_connection
from src.utils.hashing import row_hash

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# Config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "ecommerce-landing")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

if not all([MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY]):
    raise RuntimeError("Missing MinIO env vars")
    
FEED_CONFIGS = {
    "order_status/": {
        "table": "bronze.order_status",
        "columns": [
            "order_id",
            "status",
            "status_timestamp",
            "carrier",
            "tracking_number",
            "warehouse_id",
            "notes",
            "source_system",
            "file_date",
        ],
        "pk": ["order_id", "status_timestamp", "status"],
    },
    "returns/": {
        "table": "bronze.returns",
        "columns": [
            "return_id",
            "order_id",
            "product_id",
            "return_date",
            "return_reason",
            "refund_amount",
            "refund_currency",
            "return_status",
            "quantity",
            "source_system",
            "file_date",
        ],
        "pk": ["return_id"],
    },
    "inventory/": {
        "table": "bronze.inventory_snapshots",
        "columns": [
            "product_id",
            "warehouse_id",
            "snapshot_date",
            "quantity_on_hand",
            "quantity_reserved",
            "quantity_available",
            "reorder_point",
            "category",
            "is_stockout",
            "source_system",
            "file_date",
        ],
        "pk": ["product_id", "warehouse_id", "snapshot_date"],
    },
}


def get_minio_client() -> Minio:
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def _file_checksum(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _already_processed(source_system: str, file_name: str, checksum: str) -> bool:
    sql = """
        SELECT 1
        FROM logs.file_ingestion_log
        WHERE source_system = %s
          AND file_name = %s
          AND file_checksum = %s
          AND status = 'processed'
        LIMIT 1
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_system, file_name, checksum))
            return cur.fetchone() is not None


def _log_file_ingestion(
    source_system: str,
    file_name: str,
    file_path: str,
    file_size: int,
    checksum: str,
    status: str,
    rows_loaded: int,
    run_id: str,
) -> None:
    sql = """
        INSERT INTO logs.file_ingestion_log (
            source_system, file_name, file_path, file_size_bytes,
            file_checksum, status, rows_loaded, run_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_system, file_name, file_checksum) DO UPDATE
        SET status = EXCLUDED.status,
            rows_loaded = EXCLUDED.rows_loaded,
            loaded_at = NOW(),
            run_id = EXCLUDED.run_id
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    source_system,
                    file_name,
                    file_path,
                    file_size,
                    checksum,
                    status,
                    rows_loaded,
                    run_id,
                ),
            )


def _prepare_rows(
    df: pl.DataFrame,
    business_cols: list[str],
    batch_id: str,
    file_name: str,
) -> list[tuple]:
    now = datetime.now(timezone.utc)
    cols = [c for c in business_cols if c in df.columns]
    rows = []

    for record in df.select(cols).iter_rows(named=True):
        values = [record.get(c) for c in cols]
        h = row_hash(values)
        full_row = tuple(values) + (
            "minio",        # source_system
            now,            # loaded_at
            batch_id,       # batch_id
            file_name,      # file_name
            h,              # row_hash
        )
        rows.append(full_row)

    return rows


def load_one_file(
    client: Minio,
    object_name: str,
    config: dict,
    batch_id: str,
    run_id: str,
) -> dict:
    table = config["table"]
    columns = config["columns"]
    pk = config["pk"]

    print(f"\n→ Processing s3://{MINIO_BUCKET}/{object_name}")

    try:
        response = client.get_object(MINIO_BUCKET, object_name)
        data = response.read()
        response.close()
        response.release_conn()
    except S3Error as e:
        print(f"  ✗ Failed to download: {e}")
        return {"file": object_name, "status": "failed", "error": str(e)}

    checksum = _file_checksum(data)
    file_size = len(data)
    file_name = object_name.split("/")[-1]

    if _already_processed("minio", file_name, checksum):
        print(f"  ⏭ Already processed (checksum match) — skipping")
        return {
            "file": object_name,
            "status": "skipped",
            "reason": "already_processed",
            "rows": 0,
        }

    # Parse CSV with Polars
    try:
        df = pl.read_csv(io.BytesIO(data), infer_schema_length=10000, ignore_errors=True)
    except Exception as e:
        print(f"  ✗ Failed to parse CSV: {e}")
        _log_file_ingestion(
            "minio", file_name, object_name, file_size, checksum, "failed", 0, run_id
        )
        return {"file": object_name, "status": "failed", "error": str(e)}

    print(f"  Rows in file: {df.height:,}")

    if df.height == 0:
        _log_file_ingestion(
            "minio", file_name, object_name, file_size, checksum, "processed", 0, run_id
        )
        return {"file": object_name, "status": "success", "rows": 0}

    insert_cols = [c for c in columns if c in df.columns] + [
        "_source_system",
        "_loaded_at",
        "_batch_id",
        "_file_name",
        "_row_hash",
    ]
    rows = _prepare_rows(df, columns, batch_id, file_name)

    conflict_target = ", ".join(pk)
    col_list = ", ".join(insert_cols)

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES %s
        ON CONFLICT ({conflict_target}) DO NOTHING
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=5000)

        _log_file_ingestion(
            "minio",
            file_name,
            object_name,
            file_size,
            checksum,
            "processed",
            len(rows),
            run_id,
        )
        print(f"  ✓ Loaded into {table}")
        return {"file": object_name, "status": "success", "rows": len(rows)}

    except Exception as e:
        print(f"  ✗ Database error: {e}")
        _log_file_ingestion(
            "minio", file_name, object_name, file_size, checksum, "failed", 0, run_id
        )
        return {"file": object_name, "status": "failed", "error": str(e)}


def load_all_minio_feeds(batch_id: str | None = None) -> list[dict]:
    if batch_id is None:
        batch_id = (
            f"minio_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_"
            f"{uuid.uuid4().hex[:8]}"
        )
    run_id = batch_id

    print("=" * 60)
    print("MinIO → Bronze loader")
    print(f"Batch / Run ID : {batch_id}")
    print(f"Bucket         : {MINIO_BUCKET}")
    print(f"Endpoint       : {MINIO_ENDPOINT}")
    print("=" * 60)

    client = get_minio_client()

    if not client.bucket_exists(MINIO_BUCKET):
        raise RuntimeError(
            f"Bucket '{MINIO_BUCKET}' does not exist. "
            "Run scripts/upload_seed_to_minio.py first."
        )

    results = []

    for prefix, config in FEED_CONFIGS.items():
        print(f"\n--- Feed: {prefix} → {config['table']} ---")
        objects = client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)

        file_count = 0
        for obj in objects:
            if not obj.object_name.endswith(".csv"):
                continue
            file_count += 1
            result = load_one_file(client, obj.object_name, config, batch_id, run_id)
            results.append(result)

        if file_count == 0:
            print(f"  No CSV files found under {prefix}")

    print("\n" + "=" * 60)
    print("Summary")
    success = sum(1 for r in results if r["status"] == "success")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed = sum(1 for r in results if r["status"] == "failed")
    print(f"  Success : {success}")
    print(f"  Skipped : {skipped}")
    print(f"  Failed  : {failed}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    load_all_minio_feeds()