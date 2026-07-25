# 🚕 NYC Taxi Pipeline

A Dockerized ETL pipeline that downloads the NYC Yellow Taxi dataset from Kaggle and efficiently loads it into a PostgreSQL database using PostgreSQL's high-performance `COPY` command.

This project demonstrates fundamental Data Engineering concepts, including data ingestion, bulk loading, environment management, logging, and containerized database deployment.

---

## 📌 Project Overview

This pipeline automates the process of ingesting NYC Yellow Taxi trip data into PostgreSQL.

### Pipeline Workflow

1. Download the NYC Yellow Taxi dataset from Kaggle.
2. Launch PostgreSQL using Docker Compose.
3. Create the `trips` table if it doesn't already exist.
4. Load all CSV files into PostgreSQL using the optimized `COPY` command.
5. Log execution details, inserted row counts, and errors.

---

## 🏗️ Architecture

```text
                 Kaggle Dataset
                       │
                       ▼
              download_dataset.py
                       │
             Downloads CSV Files
                       │
                       ▼
               db_pipeline.py
                       │
        Reads Environment Variables
                       │
                       ▼
             PostgreSQL (Docker)
                       │
            Bulk Insert using COPY
                       │
                       ▼
                 trips Table
```

---

## 🚀 Features

- ✅ Automated dataset download from Kaggle
- ✅ Dockerized PostgreSQL database
- ✅ Automatic table creation
- ✅ Bulk CSV ingestion using PostgreSQL COPY
- ✅ Environment variable configuration
- ✅ Logging and execution monitoring
- ✅ Transaction rollback on failure
- ✅ Row count verification

---

## 🛠️ Tech Stack

- Python
- PostgreSQL
- Docker
- Docker Compose
- psycopg2
- python-dotenv
- KaggleHub

---

## 📂 Project Structure

```text
nyc-taxi-pipeline/
│
├── dataset.py
├── db_pipeline.py
├── docker-compose.yml
├── requirements.txt
├── .gitignore
├── .env.example
├── README.md
└── LICENSE
```

---

## ⚙️ Installation

### 1. Clone the Repository

```bash
git clone https://github.com/UzairKhan07/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Create a `.env` file in the project root.

```env
PGHOST=localhost
PGDATABASE=yellowtaxi
PGUSER=your_username
PGPASSWORD=your_password
```

### 4. Start PostgreSQL

```bash
docker-compose up -d
```

### 5. Download the Dataset

```bash
python dataset.py
```

### 6. Run the ETL Pipeline

```bash
python db_pipeline.py
```

---

## 📊 Pipeline Workflow

```text
Download Dataset
        │
        ▼
Locate CSV Files
        │
        ▼
Create Database Table
        │
        ▼
Bulk Load using COPY
        │
        ▼
Commit Transaction
        │
        ▼
Generate Execution Logs
```

---

## 📈 Example Output

```text
Table 'trips' ensured.

Starting load for yellow_tripdata_2015-01.csv

Rows before: 0

Rows after: 12,748,986

Pipeline completed successfully.
```

---

## 📚 Dataset

**NYC Yellow Taxi Trip Records**

Source:
https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data

---

## 🔮 Future Improvements

- Apache Airflow orchestration
- Incremental data loading
- Data quality validation
- Table partitioning
- Dockerized Python application
- Apache Spark integration
- Azure Data Factory pipeline
- GitHub Actions CI/CD
- Unit and integration testing

---

## 🤝 Contributing

Contributions, suggestions, and improvements are welcome.

If you'd like to improve this project, feel free to fork the repository and submit a pull request.

---

## 👨‍💻 Author

**Uzair Khan**

Data Engineer | Python | SQL | PostgreSQL | Docker | ETL | Data Warehousing | Power BI

GitHub: https://github.com/UzairKhan07

---

## ⭐ Support

If you found this project helpful, please consider giving it a ⭐ on GitHub.