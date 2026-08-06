from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import glob
import hashlib
import os
import logging
import tempfile

DATA_DIR = "/opt/airflow/data"

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# order must match the source csvs (no header name matching, just position)
CANONICAL_COLUMNS = [
    "vendorid", "pickup_datetime", "dropoff_datetime", "passenger_count",
    "trip_distance", "pickup_longitude", "pickup_latitude", "ratecodeid",
    "store_and_fwd_flag", "dropoff_longitude", "dropoff_latitude",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
]

# columns that define a "unique" trip, used for dedup + hash
HASH_COLUMNS = [
    "vendorid", "pickup_datetime", "dropoff_datetime",
    "passenger_count", "trip_distance", "fare_amount",
]

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS trips_staging (
    vendorid INT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    pickup_longitude FLOAT,
    pickup_latitude FLOAT,
    ratecodeid INT,
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
    total_amount FLOAT,
    source_file VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS trips (
    trip_hash CHAR(32) PRIMARY KEY,
    vendorid INT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    pickup_longitude FLOAT,
    pickup_latitude FLOAT,
    ratecodeid INT,
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
    total_amount FLOAT,
    source_file VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS loaded_files (
    filename VARCHAR(255) PRIMARY KEY,
    file_checksum CHAR(32) NOT NULL,
    rows_staged INT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    dag_run_id VARCHAR(255),
    task_stage VARCHAR(100),
    rows_processed INT,
    status VARCHAR(50),
    run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

TRANSFORM_SQL = """
DROP TABLE IF EXISTS trips_clean;

CREATE TABLE trips_clean AS
SELECT
    trip_hash,
    vendorid,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount,
    ROUND((EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60.0)::numeric, 2)
        AS trip_duration_minutes,
    ROUND((trip_distance / NULLIF(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 3600.0, 0))::numeric, 2)
        AS avg_speed_mph,
    ROUND((tip_amount / NULLIF(fare_amount, 0) * 100)::numeric, 2)
        AS tip_percentage,
    EXTRACT(HOUR FROM pickup_datetime)::int AS pickup_hour,
    TRIM(TO_CHAR(pickup_datetime, 'Day')) AS pickup_day_of_week,
    (EXTRACT(DOW FROM pickup_datetime) IN (0, 6)) AS is_weekend
FROM trips;

CREATE INDEX IF NOT EXISTS idx_trips_clean_pickup ON trips_clean (pickup_datetime);
"""


def _file_checksum(path, chunk_size=8 * 1024 * 1024):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def download_dataset(**kwargs):
    import kagglehub
    os.makedirs(DATA_DIR, exist_ok=True)
    path = kagglehub.dataset_download(
        "elemento/nyc-yellow-taxi-trip-data",
        output_dir=DATA_DIR,
    )
    logging.info(f"downloaded to {path}")
    return path


def ensure_tables():
    hook = PostgresHook(postgres_conn_id='taxi_postgres')
    hook.run(CREATE_TABLES_SQL)
    logging.info("tables ready: trips_staging, trips, loaded_files, pipeline_runs")


def stage_and_load(**kwargs):
    """
    One pass per file: polars filters and streams straight to a temp csv on
    disk via sink_csv - genuinely bounded memory, reads/filters/writes in
    chunks the whole way through, never holds the full file in RAM. That temp
    file then gets COPY'd into postgres and deleted. In-file dedup is not
    done here - trip_hash + ON CONFLICT on the postgres side already handles
    duplicates at insert time, so doing it twice would be wasted work.
    """
    import polars as pl

    hook = PostgresHook(postgres_conn_id='taxi_postgres')
    conn = hook.get_conn()
    cur = conn.cursor()

    already_loaded = {
        row[0]: row[1]
        for row in hook.get_records("SELECT filename, file_checksum FROM loaded_files;")
    }

    files = glob.glob(os.path.join(DATA_DIR, "**", "yellow_tripdata_*.csv"), recursive=True)
    if not files:
        raise FileNotFoundError(f"no csv files under {DATA_DIR}")

    cur.execute("TRUNCATE TABLE trips_staging;")
    conn.commit()

    copy_sql = f"""
        COPY trips_staging ({", ".join(CANONICAL_COLUMNS)}, source_file)
        FROM STDIN WITH CSV HEADER
    """

    processed = []
    for file in files:
        filename = os.path.basename(file)
        checksum = _file_checksum(file)

        if already_loaded.get(filename) == checksum:
            logging.info(f"skipping {filename}, checksum unchanged")
            continue

        logging.info(f"processing {filename}")
        lf = pl.scan_csv(file, try_parse_dates=True, ignore_errors=True)
        col_names = lf.collect_schema().names()
        lf = lf.rename(dict(zip(col_names, CANONICAL_COLUMNS[: len(col_names)])))
        lf = lf.filter(
            (pl.col("trip_distance") > 0)
            & (pl.col("fare_amount") > 0)
            & (pl.col("passenger_count") > 0)
            & (pl.col("dropoff_datetime") > pl.col("pickup_datetime"))
        )
        lf = lf.with_columns(pl.lit(filename).alias("source_file"))

        tmp_path = os.path.join(tempfile.gettempdir(), f"staged_{filename}")
        lf.sink_csv(tmp_path)

        rows = pl.scan_csv(tmp_path).select(pl.len()).collect().item()

        with open(tmp_path, "r") as f:
            cur.copy_expert(copy_sql, f)
        conn.commit()
        os.remove(tmp_path)

        logging.info(f"{filename}: {rows} rows staged")
        processed.append((filename, checksum, rows))

    if not processed:
        logging.info("nothing new, all files already processed")
        kwargs['ti'].xcom_push(key='rows_inserted', value=0)
        cur.close()
        conn.close()
        return

    hash_expr = " || ".join(f"{col}::text" for col in HASH_COLUMNS)
    insert_sql = f"""
        INSERT INTO trips (trip_hash, {", ".join(CANONICAL_COLUMNS)}, source_file)
        SELECT md5({hash_expr}), {", ".join(CANONICAL_COLUMNS)}, source_file
        FROM trips_staging
        ON CONFLICT (trip_hash) DO NOTHING;
    """
    cur.execute(insert_sql)
    rows_inserted = cur.rowcount
    conn.commit()

    for filename, checksum, rows in processed:
        cur.execute(
            """
            INSERT INTO loaded_files (filename, file_checksum, rows_staged)
            VALUES (%s, %s, %s)
            ON CONFLICT (filename)
            DO UPDATE SET file_checksum = EXCLUDED.file_checksum,
                          rows_staged = EXCLUDED.rows_staged,
                          loaded_at = CURRENT_TIMESTAMP;
            """,
            (filename, checksum, rows),
        )
    conn.commit()

    logging.info(f"inserted {rows_inserted} new rows (dupes skipped via hash)")
    kwargs['ti'].xcom_push(key='rows_inserted', value=rows_inserted)
    cur.close()
    conn.close()


def dq_check_bronze():
    hook = PostgresHook(postgres_conn_id='taxi_postgres')

    total = hook.get_first("SELECT COUNT(*) FROM trips;")[0]
    if total == 0:
        raise ValueError("trips is empty")

    distinct = hook.get_first("SELECT COUNT(DISTINCT trip_hash) FROM trips;")[0]
    if distinct != total:
        raise ValueError(f"{total - distinct} duplicate trip_hash values in trips")

    nulls = hook.get_first(
        "SELECT COUNT(*) FROM trips WHERE pickup_datetime IS NULL OR dropoff_datetime IS NULL;"
    )[0]
    if nulls > 0:
        raise ValueError(f"{nulls} rows with null pickup/dropoff")

    logging.info(f"bronze dq passed, {total} rows, no dupes, no null keys")


def transform_trips():
    hook = PostgresHook(postgres_conn_id='taxi_postgres')
    hook.run(TRANSFORM_SQL)
    clean_count = hook.get_first("SELECT COUNT(*) FROM trips_clean;")[0]
    logging.info(f"trips_clean rebuilt, {clean_count} rows")


def dq_check_silver(**kwargs):
    hook = PostgresHook(postgres_conn_id='taxi_postgres')

    total = hook.get_first("SELECT COUNT(*) FROM trips_clean;")[0]
    if total == 0:
        raise ValueError("trips_clean is empty")

    bad_duration = hook.get_first(
        "SELECT COUNT(*) FROM trips_clean WHERE trip_duration_minutes <= 0;"
    )[0]
    if bad_duration > 0:
        raise ValueError(f"{bad_duration} rows with non-positive duration")

    distinct = hook.get_first("SELECT COUNT(DISTINCT trip_hash) FROM trips_clean;")[0]
    if distinct != total:
        raise ValueError(f"{total - distinct} duplicate trip_hash values in trips_clean")

    logging.info(f"silver dq passed, {total} rows")

    hook.run(
        """
        INSERT INTO pipeline_runs (dag_run_id, task_stage, rows_processed, status)
        VALUES (%s, %s, %s, %s);
        """,
        parameters=(kwargs['run_id'], 'dq_check_silver', total, 'success'),
    )


with DAG(
    dag_id='nyc_taxi_etl',
    default_args=default_args,
    description='NYC taxi pipeline: polars streaming stage, hash-based idempotent load, sql transform',
    schedule_interval=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['etl', 'postgres', 'kaggle', 'portfolio'],
) as dag:

    download = PythonOperator(task_id='download_dataset', python_callable=download_dataset)
    create_tables = PythonOperator(task_id='ensure_tables', python_callable=ensure_tables)
    load = PythonOperator(task_id='stage_and_load', python_callable=stage_and_load)
    dq_bronze = PythonOperator(task_id='dq_check_bronze', python_callable=dq_check_bronze)
    transform = PythonOperator(task_id='transform_trips', python_callable=transform_trips)
    dq_silver = PythonOperator(task_id='dq_check_silver', python_callable=dq_check_silver)

    download >> create_tables >> load >> dq_bronze >> transform >> dq_silver
