# 🚖 NYC Taxi ETL Pipeline with Apache Airflow

A production-inspired Data Engineering project built using **Apache Airflow, PostgreSQL, Docker, Polars, and Python**.

The pipeline downloads the NYC Yellow Taxi dataset from Kaggle, performs staged ingestion, data quality validation, transformations, and loads the cleaned data into PostgreSQL using a **Medallion-inspired architecture**.

This project is part of my **90-Day Data Engineering Roadmap**, where each week focuses on learning real-world tools and engineering practices used in modern data platforms.

---

# 📖 Overview

This project extends the Week 3 PostgreSQL ETL pipeline by introducing **Apache Airflow** for orchestration and significantly refactoring the ingestion workflow.

Unlike the original implementation, this version introduces:

- Apache Airflow orchestration
- Streaming ingestion using Polars
- Hash-based idempotent loading
- Bronze & Silver data layers
- Automated Data Quality checks
- Modular ETL stages
- Dockerized development environment

The goal is to build an ETL pipeline that resembles production engineering practices rather than a simple data loading script.

---

# ✨ Features

- ✅ Apache Airflow DAG orchestration
- ✅ Dockerized deployment
- ✅ PostgreSQL data warehouse
- ✅ Kaggle dataset integration
- ✅ Streaming CSV processing using Polars
- ✅ Hash-based idempotent loading
- ✅ Bronze Layer implementation
- ✅ Silver Layer transformations
- ✅ Data Quality validation for both layers
- ✅ Automatic retries and task logging
- ✅ Custom Airflow Docker image
- ✅ Environment variables managed with `.env`

---

# 🏛️ Pipeline Architecture

```text
                    Kaggle Dataset
                          │
                          ▼
                 Download Dataset
                          │
                          ▼
               Bronze Layer (Raw)
         -----------------------------
         trips_staging
                │
                ▼
              trips
(Hash-based deduplication & Idempotent Load)
                          │
                Bronze DQ Checks
                          │
                          ▼
             Silver Layer (Processed)
         -----------------------------
               trips_clean
      Business-friendly transformations
                          │
                Silver DQ Checks
                          │
                          ▼
                  PostgreSQL

      Gold Layer (Planned Future Enhancement)
```

---

# ⚙️ Pipeline Workflow

The Airflow DAG executes the following tasks:

1. Download the NYC Yellow Taxi dataset from Kaggle.
2. Create all required database tables.
3. Stream CSV files into the staging table using Polars.
4. Load only new records into the Bronze table using MD5 hash-based deduplication.
5. Execute Bronze layer Data Quality checks.
6. Transform Bronze data into the Silver layer.
7. Execute Silver layer Data Quality checks.
8. Log pipeline execution metadata.

---

# 🛠️ Tech Stack

| Category | Technology |
|-----------|------------|
| Language | Python |
| Workflow | Apache Airflow |
| Database | PostgreSQL |
| Data Processing | Polars |
| Containerization | Docker & Docker Compose |
| Dataset | Kaggle NYC Yellow Taxi Dataset |
| Database Client | pgAdmin |
| Version Control | Git & GitHub |
| Operating System | Ubuntu (WSL2) |

---

# 📂 Project Structure

```text
week4-airflow-taxi-pipeline/
│
├── dags/
│   └── nyc_taxi_etl_dag.py
│
├── data/
├── logs/
├── plugins/
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── README.md
```

---

# 🚀 Getting Started

## Clone the Repository

```bash
git clone https://github.com/UzairKhan07/90-day-data-engineering.git

cd 90-day-data-engineering/week4-airflow-taxi-pipeline
```

---

## Configure Environment Variables

```bash
cp .env.example .env
```

Configure:

- Airflow credentials
- PostgreSQL credentials
- pgAdmin credentials
- Kaggle API credentials

---

## Build the Custom Airflow Image

```bash
docker compose build
```

---

## Start the Services

```bash
docker compose up -d
```

---

## Access the Services

| Service | URL |
|----------|-----|
| Airflow UI | http://localhost:8081 |
| pgAdmin | http://localhost:8080 |

---

## Run the Pipeline

Enable the **nyc_taxi_etl** DAG from the Airflow UI and trigger it manually.

Airflow will orchestrate the entire ETL process from ingestion through transformation and validation.

---

# 🔄 Version 2 Improvements

Compared to the original Week 4 implementation, this version introduces major architectural improvements.

### Engineering Improvements

- Refactored into a Medallion-inspired architecture
- Implemented Bronze and Silver layers
- Introduced staging tables
- Added hash-based idempotent loading
- Added file checksum tracking
- Added pipeline execution logging
- Added Bronze Data Quality validation
- Added Silver Data Quality validation
- Migrated to Polars streaming for efficient CSV processing
- Built a custom Airflow Docker image
- Improved project modularity and maintainability

---

# ⚡ Development Environment Migration

During development, Docker and Apache Airflow experienced significant performance degradation when the project was stored on the Windows-mounted filesystem.

To improve filesystem performance and eliminate Linux permission inconsistencies, development was migrated to the native **Ubuntu (WSL2)** filesystem.

### Benefits

- Faster Docker file I/O
- Faster DAG execution
- Better Linux file permission handling
- More stable Airflow scheduler
- Development environment closer to production Linux deployments

---

# 🧠 Engineering Challenges

## Docker Performance

**Challenge**

Running Docker volumes from the Windows filesystem resulted in slower Airflow execution.

**Solution**

Migrated development to the native Ubuntu (WSL2) filesystem.

---

## Linux File Permissions

**Challenge**

Airflow containers initially failed to write logs and mounted volumes because of permission conflicts.

**Solution**

Updated volume permissions and aligned container user ownership.

---

## Idempotent ETL

**Challenge**

Re-running the pipeline could duplicate records.

**Solution**

Implemented MD5-based trip hashing together with PostgreSQL `ON CONFLICT DO NOTHING` to safely support repeated DAG executions.

---

## Efficient Large File Processing

**Challenge**

Loading multi-GB CSV files into memory is inefficient.

**Solution**

Used Polars LazyFrame together with streaming CSV writes before bulk loading into PostgreSQL via `COPY`.

---

# 📈 Future Improvements

- Implement the **Gold Layer** for business-ready analytical datasets and reporting
- Dynamic Task Mapping (one Airflow task per dataset)
- Great Expectations for advanced Data Quality validation
- Incremental processing for newly arriving datasets
- Automated unit testing for DAGs
- CI/CD using GitHub Actions
- Data lineage and metadata tracking
- Monitoring and alerting
- Cloud deployment (AWS/GCP/Azure)

---

# 👨‍💻 Author

**Muhammad Uzair Khan**

Business Analyst • Aspiring Data Engineer • Future AI Engineer

GitHub: **https://github.com/UzairKhan07**

---

# 📚 90-Day Data Engineering Roadmap

This project is part of my personal **90-Day Data Engineering Roadmap**.

The objective of this roadmap is to transition from Business Intelligence and Analytics into Data Engineering by building increasingly production-inspired projects while learning industry-standard tools, architectures, and engineering practices.