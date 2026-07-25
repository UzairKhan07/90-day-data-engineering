# 🌀 NYC Taxi Pipeline — Airflow Orchestration

Extends the [Week 3 Docker + PostgreSQL pipeline](../Week%203%20Docker%20+%20PostgreSQL) by replacing a manually-run Python script with an Apache Airflow DAG: download → create table → bulk load, fully orchestrated, retried, and logged per task.

---

## 📌 Overview

Week 3 proved the core mechanic (bulk-loading NYC Yellow Taxi CSVs into Postgres via `COPY`). Week 4 wraps that logic in Airflow so it's:

- **Orchestrated** — explicit task dependencies instead of top-to-bottom script execution
- **Observable** — per-task logs, retries, and status in the Airflow UI instead of console output
- **Isolated** — Airflow's own metadata lives in a separate Postgres instance from the actual taxi data

### Pipeline Workflow

1. `download_dataset` — pulls the NYC Yellow Taxi dataset from Kaggle into a mounted volume
2. `ensure_table` — creates the `trips` table if it doesn't exist
3. `load_files` — streams each CSV into Postgres via `COPY`, with per-file commit/rollback and row-count logging

---

## 🏗️ Architecture

```text
                    Kaggle Dataset
                          │
                          ▼
              download_dataset (PythonOperator)
                          │
                          ▼
               ensure_table (PostgresHook)
                          │
                          ▼
                load_files (COPY, streamed)
                          │
                          ▼
                  postgres-taxi :: trips
```

Two isolated Postgres instances run side by side:

| Instance | Purpose | Port |
|---|---|---|
| `postgres-airflow` | Airflow's own metadata (DAG runs, task state, users) | 5433 |
| `postgres-taxi` | The actual `trips` data | 5432 |

---

## 🚀 Features

- ✅ Dockerized Airflow (webserver + scheduler + one-shot init service)
- ✅ Automated Kaggle dataset download inside the DAG
- ✅ Bulk CSV ingestion via Postgres `COPY` (streamed, not loaded into memory)
- ✅ Per-task retries and structured logging
- ✅ Secrets externalized to `.env` (nothing sensitive committed)
- ✅ Healthchecks + `service_completed_successfully` gating so services don't race each other on startup

---

## 🛠️ Tech Stack

Apache Airflow · PostgreSQL · Docker · Docker Compose · psycopg2 · kagglehub · pgAdmin

---

## 📂 Project Structure

```text
week4-airflow-taxi-pipeline/
├── dags/
│   └── nyc_taxi_etl_dag.py
├── data/              # gitignored — populated at runtime
├── logs/              # gitignored
├── plugins/
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

---

## ⚙️ Setup

### 1. Clone and configure

```bash
git clone https://github.com/UzairKhan07/90-day-data-engineering.git
cd 90-day-data-engineering/week4-airflow-taxi-pipeline
cp .env.example .env
```

Fill in `.env` with your own Postgres/pgAdmin/Airflow admin credentials and your [Kaggle API token](https://www.kaggle.com/settings) (Account → Create New API Token).

### 2. Start everything

```bash
docker-compose up -d
```

`airflow-init` runs once (migrates the metadata DB, creates the admin user), then exits. The webserver and scheduler wait for it to finish before starting.

### 3. Verify

```bash
docker-compose logs airflow-init
docker ps
```

### 4. Access

- Airflow UI: [http://localhost:8081](http://localhost:8081) (login from your `.env`)
- pgAdmin: [http://localhost:8080](http://localhost:8080)

### 5. Run the DAG

Enable `nyc_taxi_etl` in the Airflow UI and trigger it manually. It's not scheduled (`schedule_interval=None`) since it's pulling a static historical dataset, not a recurring feed.

### 6. Verify the load

```bash
docker exec -it dev-postgres psql -U $POSTGRES_TAXI_USER -d yellowtaxi -c "SELECT COUNT(*) FROM trips;"
```

---

## ⚠️ Known Limitations

- **Not idempotent** — re-running the DAG duplicates rows. No upsert or dedup logic yet.
- **Full dataset download** — `download_dataset` pulls the entire multi-year Kaggle dataset (several GB) rather than a single file. Fine for a learning exercise; worth scoping down for repeated runs.
- **`_PIP_ADDITIONAL_REQUIREMENTS`** installs Python deps at container startup — convenient for development, but slow and non-persistent. A custom Airflow image with deps baked in is the production-grade approach.

---

## 🔮 Future Improvements

- [ ] Upsert logic (`ON CONFLICT`) for idempotent reruns
- [ ] Data quality checks between load and table (nulls, ranges) as a dedicated task
- [ ] Custom Docker image instead of `_PIP_ADDITIONAL_REQUIREMENTS`
- [ ] Dynamic task mapping — one mapped task instance per CSV file instead of a single loop
- [ ] CI (GitHub Actions) to lint the DAG and validate `docker-compose config` on push

---

## 👨‍💻 Author

**Muhammad Uzair Khan**

Data Analyst | Python · SQL · Airflow · Docker · PostgreSQL · Power BI

GitHub: [github.com/UzairKhan07](https://github.com/UzairKhan07)

Part of a 90-day self-directed Data Engineering roadmap.
