from __future__ import annotations
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests
from psycopg2.extras import execute_values
from src.utils.db import get_connection
from src.utils.hashing import row_hash

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# Config
API_BASE = os.getenv("CURRENCY_API_BASE_URL")
BASE_CURRENCY = "USD"
TARGET_CURRENCIES = ["EUR", "GBP", "CAD", "AUD", "JPY", "INR", "MXN", "BRL"]
LOOKBACK_DAYS = 30


def _fetch_rates_for_date(rate_date: str) -> list[dict]:
    url = f"{API_BASE}/{rate_date}"
    params = {
        "from": BASE_CURRENCY,
        "to": ",".join(TARGET_CURRENCIES),
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    rates = payload.get("rates", {})
    actual_date = payload.get("date", rate_date)

    rows = []
    for target, rate in rates.items():
        rows.append(
            {
                "rate_date": actual_date,
                "base_currency": BASE_CURRENCY,
                "target_currency": target,
                "rate": float(rate),
            }
        )
    return rows


def _prepare_db_rows(records: list[dict], batch_id: str) -> list[tuple]:
    now = datetime.now(timezone.utc)
    rows = []

    for r in records:
        values = [
            r["rate_date"],
            r["base_currency"],
            r["target_currency"],
            r["rate"],
        ]
        h = row_hash(values)
        full_row = tuple(values) + (
            "frankfurter",  # source_system
            now,            # loaded_at
            batch_id,       # batch_id
            h,              # row_hash
        )
        rows.append(full_row)

    return rows


def load_currency_rates(
    start_date: str | None = None,
    end_date: str | None = None,
    batch_id: str | None = None,
) -> dict:
    if batch_id is None:
        batch_id = (
            f"fx_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_"
            f"{uuid.uuid4().hex[:8]}"
        )

    today = datetime.now(timezone.utc).date()
    if end_date is None:
        end_date = today.isoformat()
    if start_date is None:
        start_date = (today - timedelta(days=LOOKBACK_DAYS)).isoformat()

    print("=" * 60)
    print("Currency API → Bronze loader")
    print(f"Batch ID   : {batch_id}")
    print(f"API        : {API_BASE}")
    print(f"Base       : {BASE_CURRENCY}")
    print(f"Targets    : {', '.join(TARGET_CURRENCIES)}")
    print(f"Date range : {start_date} → {end_date}")
    print("=" * 60)

    # list of dates to fetch
    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()
    dates = []
    current = start
    while current <= end:
        dates.append(current.isoformat())
        current += timedelta(days=1)

    all_records = []
    errors = []

    for d in dates:
        try:
            records = _fetch_rates_for_date(d)
            all_records.extend(records)
            print(f"  ✓ {d} → {len(records)} rates")
        except Exception as e:
            print(f"  ✗ {d} → {e}")
            errors.append({"date": d, "error": str(e)})

    if not all_records:
        print("No rates fetched. Exiting.")
        return {"status": "failed", "rows": 0, "errors": errors}

    print(f"\nTotal rate rows collected: {len(all_records):,}")

    db_rows = _prepare_db_rows(all_records, batch_id)

    sql = """
        INSERT INTO bronze.currency_rates (
            rate_date, base_currency, target_currency, rate,
            _source_system, _loaded_at, _batch_id, _row_hash
        )
        VALUES %s
        ON CONFLICT (rate_date, base_currency, target_currency) DO NOTHING
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, db_rows, page_size=1000)

    print(f"✓ Loaded into bronze.currency_rates (batch={batch_id})")

    return {
        "status": "success",
        "rows_attempted": len(db_rows),
        "batch_id": batch_id,
        "errors": errors,
    }


if __name__ == "__main__":
    load_currency_rates()