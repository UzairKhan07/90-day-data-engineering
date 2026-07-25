from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import glob
import os
import logging

DATA_DIR = "/opt/airflow/data"

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trips (
    VendorID INT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    pickup_longitude FLOAT,
    pickup_latitude FLOAT,
    RateCodeID INT,
    store_and_fwd_flag VARCHAR(1),
    dropoff_longitude FLOAT,
    dropoff_latitude FLOAT,
    payment_type VARCHAR(20),
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT
)
"""

COPY_SQL = """
COPY trips (
    VendorID, pickup_datetime, dropoff_datetime, passenger_count,
    trip_distance, pickup_longitude, pickup_latitude, RateCodeID,
    store_and_fwd_flag, dropoff_longitude, dropoff_latitude,
    payment_type, fare_amount, extra, mta_tax, tip_amount,
    tolls_amount, improvement_surcharge, total_amount
)
FROM STDIN WITH CSV HEADER
"""


def download_dataset():
    import kagglehub
    os.makedirs(DATA_DIR, exist_ok=True)
    path = kagglehub.dataset_download(
        "elemento/nyc-yellow-taxi-trip-data",
        output_dir=DATA_DIR,
    )
    logging.info(f"Dataset downloaded to {path}")
    return path


def ensure_table():
    hook = PostgresHook(postgres_conn_id='taxi_postgres')
    hook.run(CREATE_TABLE_SQL)
    logging.info("Table 'trips' ensured.")


def load_files():
    hook = PostgresHook(postgres_conn_id='taxi_postgres')
    conn = hook.get_conn()
    cur = conn.cursor()

    def get_count():
        cur.execute("SELECT COUNT(*) FROM trips;")
        return cur.fetchone()[0]

    files = glob.glob(os.path.join(DATA_DIR, "**", "yellow_tripdata_*.csv"), recursive=True)
    if not files:
        raise FileNotFoundError(f"No CSV files found under {DATA_DIR}")

    for file in files:
        size_mb = os.path.getsize(file) / (1024 * 1024)
        logging.info(f"Starting load for {file} size: {size_mb:.2f} MB")
        before = get_count()
        try:
            with open(file, "r") as f:
                cur.copy_expert(COPY_SQL, f)
            conn.commit()
            after = get_count()
            logging.info(f"Loaded {file}. Rows before: {before}, after: {after}, added: {after - before}")
        except Exception as e:
            conn.rollback()
            logging.error(f"Error loading {file}: {e}")
            raise

    logging.info(f"Final row count in trips: {get_count()}")
    cur.close()
    conn.close()


with DAG(
    dag_id='nyc_taxi_etl',
    default_args=default_args,
    description='Download NYC taxi data from Kaggle and bulk-load into Postgres',
    schedule_interval=None,  # manual trigger — this is a full historical dataset, not a daily feed
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['etl', 'postgres', 'kaggle'],
) as dag:

    download = PythonOperator(task_id='download_dataset', python_callable=download_dataset)
    create_table = PythonOperator(task_id='ensure_table', python_callable=ensure_table)
    load = PythonOperator(task_id='load_files', python_callable=load_files)

    download >> create_table >> load
