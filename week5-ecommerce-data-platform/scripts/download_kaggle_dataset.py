# Download the E-Commerce U.S. Dataset from Kaggle.

from __future__ import annotations
import os
import zipfile
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# Config
DATASET = "limjeongeun/synthetic-u-s-e-commerce-dataset-1m-orders"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw" / "kaggle"

# Download and unzip the Kaggle dataset.
def download_dataset(force: bool = False) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    orders_file = RAW_DIR / "orders.csv"
    if orders_file.exists() and not force:
        print(f"✓ Dataset already present at {RAW_DIR}")
        print(f"  orders.csv size: {orders_file.stat().st_size / 1e6:.1f} MB")
        return RAW_DIR

    print(f"Downloading dataset: {DATASET}")
    print(f"Target directory  : {RAW_DIR}")

    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(
            DATASET,
            path=str(RAW_DIR),
            unzip=True,
            quiet=False,
        )
    except Exception as e:
        print(f"kaggle package failed ({e}). Trying CLI fallback...")
        import subprocess

        cmd = [
            "kaggle",
            "datasets",
            "download",
            "-d",
            DATASET,
            "-p",
            str(RAW_DIR),
            "--unzip",
        ]
        subprocess.run(cmd, check=True)

    for zip_path in RAW_DIR.glob("*.zip"):
        print(f"Unzipping {zip_path.name}...")
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(RAW_DIR)
        zip_path.unlink()

    if not orders_file.exists():
        raise FileNotFoundError(
            f"Download finished but orders.csv not found in {RAW_DIR}. "
            "Check dataset structure."
        )

    print(f"\n✓ Download complete → {RAW_DIR}")
    print(f"  orders.csv size: {orders_file.stat().st_size / 1e6:.1f} MB")
    return RAW_DIR


if __name__ == "__main__":
    download_dataset(force=False)