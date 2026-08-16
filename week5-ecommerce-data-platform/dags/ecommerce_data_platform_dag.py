from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make project modules importable inside the Airflow containers
AIRFLOW_HOME = Path("/opt/airflow")
for p in [
    AIRFLOW_HOME,
    AIRFLOW_HOME / "src",
    AIRFLOW_HOME / "scripts",
    Path(__file__).resolve().parents[1],
]:
    s = str(p)
    if s not in sys.path:
        sys.path.insert(0, s)

# Fetch required env vars
def _require_env(*keys: str) -> dict[str, str]:
    values = {}
    missing = []
    for key in keys:
        val = os.getenv(key)
        if val is None or val == "":
            missing.append(key)
        else:
            values[key] = val
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
    return values


default_args = {
    "owner": "Zeus",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


# Task callables
def _download_kaggle():
    from download_kaggle_dataset import download_dataset
    download_dataset(force=False)


def _generate_seed():
    from generate_seed_data import main as generate_seed_main
    generate_seed_main()


def _upload_seed_to_minio():
    from upload_seed_to_minio import main as upload_main
    upload_main()


# Create DB + schemas + tables if they don't exist.
def _ensure_database_and_schemas():
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    cfg = _require_env(
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB",
    )

    host = cfg["POSTGRES_HOST"]
    port = int(cfg["POSTGRES_PORT"])
    user = cfg["POSTGRES_USER"]
    password = cfg["POSTGRES_PASSWORD"]
    analytics_db = cfg["POSTGRES_DB"]
    admin_db = "postgres"

    admin_conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=admin_db,
    )
    admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    with admin_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (analytics_db,))
        exists = cur.fetchone()
        if not exists:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(analytics_db)))
            print(f"Created database {analytics_db}")
        else:
            print(f"Database {analytics_db} already exists")
    admin_conn.close()

    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=analytics_db,
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        for schema in ("bronze", "silver", "gold", "logs"):
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
            )
            print(f"Schema ready: {schema}")

    sql_root = Path("/opt/airflow/sql")
    sql_files = [
        sql_root / "bronze" / "01_kaggle_tables.sql",
        sql_root / "bronze" / "02_minio_tables.sql",
        sql_root / "bronze" / "03_currency_and_logs.sql",
        sql_root / "silver" / "01_dimensions.sql",
        sql_root / "silver" / "02_facts.sql",
        sql_root / "silver" / "03_supporting.sql",
        sql_root / "gold" / "01_dimensions.sql",
        sql_root / "gold" / "02_facts.sql",
        sql_root / "gold" / "03_aggregates.sql",
    ]

    with conn.cursor() as cur:
        for path in sql_files:
            if not path.exists():
                print(f"WARNING: SQL file missing: {path}")
                continue
            cur.execute(path.read_text(encoding="utf-8"))
            print(f"Applied: {path.name}")

    conn.close()
    print("Database bootstrap complete")
    

def _kaggle_to_bronze():
    from src.ingest.kaggle_to_bronze import load_all_kaggle_tables
    load_all_kaggle_tables()


def _minio_to_bronze():
    from src.ingest.minio_to_bronze import load_all_minio_feeds
    load_all_minio_feeds()


def _currency_to_bronze():
    from src.ingest.currency_to_bronze import load_currency_rates
    load_currency_rates()


def _bronze_dq_checks():
    from src.quality.checks import run_bronze_quality_checks
    run_bronze_quality_checks(fail_on_critical=True)


def _bronze_to_silver():
    from src.transform.bronze_to_silver import run_bronze_to_silver
    run_bronze_to_silver()


def _silver_to_gold():
    from src.transform.silver_to_gold import run_silver_to_gold
    run_silver_to_gold()


def _dashboard_readiness():
    from src.utils.db import get_connection

    sqls = {
        "fact_orders": "SELECT COUNT(*) FROM gold.fact_orders",
        "daily_sales_summary": "SELECT COUNT(*) FROM gold.daily_sales_summary",
        "product_performance": "SELECT COUNT(*) FROM gold.product_performance",
        "delivery_performance": "SELECT COUNT(*) FROM gold.delivery_performance",
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            for name, query in sqls.items():
                cur.execute(query)
                count = cur.fetchone()[0]
                print(f"  {name}: {count:,} rows")
                if count == 0:
                    raise ValueError(f"Gold table/metric empty: {name}")

    print("Dashboard readiness: OK")


# DAG
with DAG(
    dag_id="ecommerce_data_platform",
    description="Week 5 Enterprise E-Commerce Data Platform – Bronze/Silver/Gold pipeline",
    default_args=default_args,
    start_date=datetime(2024, 6, 1),
    schedule=None,  # manual trigger
    catchup=False,
    max_active_runs=1,
    tags=["week5", "ecommerce", "medallion"],
) as dag:

    ensure_db = PythonOperator(
        task_id="ensure_database_and_schemas",
        python_callable=_ensure_database_and_schemas,
    )

    download_kaggle = PythonOperator(
        task_id="download_kaggle_dataset",
        python_callable=_download_kaggle,
    )

    generate_seed = PythonOperator(
        task_id="generate_seed_data",
        python_callable=_generate_seed,
    )

    upload_seed = PythonOperator(
        task_id="upload_seed_to_minio",
        python_callable=_upload_seed_to_minio,
    )

    kaggle_bronze = PythonOperator(
        task_id="ingest_kaggle_to_bronze",
        python_callable=_kaggle_to_bronze,
    )

    minio_bronze = PythonOperator(
        task_id="ingest_minio_to_bronze",
        python_callable=_minio_to_bronze,
    )

    currency_bronze = PythonOperator(
        task_id="ingest_currency_to_bronze",
        python_callable=_currency_to_bronze,
    )

    bronze_dq = PythonOperator(
        task_id="bronze_data_quality_checks",
        python_callable=_bronze_dq_checks,
    )

    silver = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=_bronze_to_silver,
    )

    gold = PythonOperator(
        task_id="transform_silver_to_gold",
        python_callable=_silver_to_gold,
    )

    dashboard_ready = PythonOperator(
        task_id="dashboard_readiness_check",
        python_callable=_dashboard_readiness,
    )

    # Orchestration
    ensure_db >> [download_kaggle, currency_bronze]
    download_kaggle >> generate_seed >> upload_seed
    download_kaggle >> kaggle_bronze
    upload_seed >> minio_bronze
    [kaggle_bronze, minio_bronze, currency_bronze] >> bronze_dq
    bronze_dq >> silver >> gold >> dashboard_ready