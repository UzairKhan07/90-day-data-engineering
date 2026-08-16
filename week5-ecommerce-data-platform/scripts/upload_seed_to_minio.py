# Upload seed_data/ files into the MinIO bucket.

from __future__ import annotations
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
load_dotenv()

# Config
SEED_DIR = Path(__file__).resolve().parent.parent / "seed_data"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "ecommerce-landing")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

if not all([MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY]):
    raise RuntimeError("Missing MinIO env vars")

def get_minio_client() -> Minio:
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"✓ Created bucket: {bucket}")
    else:
        print(f"✓ Bucket already exists: {bucket}")


def upload_directory(client: Minio, bucket: str, local_dir: Path, prefix: str = "") -> int:    
    uploaded = 0

    for path in local_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.name == ".gitkeep":
            continue

        relative = path.relative_to(SEED_DIR)
        object_name = str(relative).replace("\\", "/")

        if prefix:
            object_name = f"{prefix.rstrip('/')}/{object_name}"

        try:
            client.fput_object(
                bucket_name=bucket,
                object_name=object_name,
                file_path=str(path),
                content_type="text/csv",
            )
            print(f"  ↑ {object_name}")
            uploaded += 1
        except S3Error as e:
            print(f"  ✗ Failed to upload {object_name}: {e}")
            raise

    return uploaded


def main() -> None:
    print("=" * 60)
    print("Uploading seed data to MinIO")
    print("=" * 60)
    print(f"Endpoint : {MINIO_ENDPOINT}")
    print(f"Bucket   : {MINIO_BUCKET}")
    print(f"Source   : {SEED_DIR}")
    print()

    if not SEED_DIR.exists():
        print(f"ERROR: seed_data directory not found at {SEED_DIR}")
        sys.exit(1)

    csv_files = list(SEED_DIR.rglob("*.csv"))
    if not csv_files:
        print("ERROR: No CSV files found in seed_data/.")
        print("Run scripts/generate_seed_data.py first.")
        sys.exit(1)

    print(f"Found {len(csv_files)} CSV files to upload.\n")

    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    print("\nUploading files...")
    count = upload_directory(client, MINIO_BUCKET, SEED_DIR)

    print("\n" + "=" * 60)
    print(f"✅ Upload complete — {count} files uploaded to s3://{MINIO_BUCKET}/")
    print("=" * 60)

    print("\nObjects currently in bucket:")
    objects = client.list_objects(MINIO_BUCKET, recursive=True)
    for obj in objects:
        print(f"  - {obj.object_name}  ({obj.size:,} bytes)")


if __name__ == "__main__":
    main()