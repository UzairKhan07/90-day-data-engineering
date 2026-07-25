import psycopg2
from psycopg2.extras import RealDictCursor
import glob
import os
from dotenv import load_dotenv
load_dotenv()
import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Database connection
conn = psycopg2.connect(
    host=os.getenv("PGHOST"),
    database=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD")
)
cur = conn.cursor(cursor_factory=RealDictCursor)

# Create table if not exists
cur.execute("""
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
""")

conn.commit()
logging.info("Table 'trips' ensured.")

# row count
def get_count():
    cur.execute("SELECT COUNT(*) FROM trips;")
    return cur.fetchone()['count']

# COPY SQL
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

# Load all CSV files
for file in glob.glob(r"F:\90 Day DE\Week 3 Docker + PostgreSQL\data\yellow_tripdata_*.csv"):
    size_mb = os.path.getsize(file) / (1024*1024)
    logging.info(f"Starting load for {file} size: {size_mb:.2f} MB")
    before = get_count()
    try:
        with open(file, "r") as f:
            cur.copy_expert(COPY_SQL, f)
        conn.commit()
        after = get_count()
        logging.info(f"Loaded {file} successfully. Rows before: {before}, after: {after}, added: {after - before}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading {file}: {e}")
    
# Cleanup
logging.info(f"Final row count in trips: {get_count()}")
cur.close()
conn.close()
logging.info("Pipeline completed.")
