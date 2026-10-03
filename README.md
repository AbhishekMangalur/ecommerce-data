# E-Commerce Data Engineering Pipeline

An end-to-end local data engineering project that ingests e-commerce data, processes it using PySpark, performs data quality checks, and creates analytics-ready tables in PostgreSQL. Apache Airflow is used to orchestrate the complete pipeline.


## Directory Structure

```
ecommerce-data-engineering/
│
├── data/
│   ├── raw/         # Raw incoming data dumps (JSON, CSV, Parquet)
│   ├── processed/   # Cleaned and transformed datasets
│   └── archive/     # Historical data backups
│
├── ingestion/       # Data ingestion scripts and API clients
├── spark/           # PySpark batch and streaming processing scripts
├── dags/            # Apache Airflow DAG workflows
├── sql/             # SQL transformations, DDL, and queries
├── tests/           # Unit and integration tests
├── config/          # Project configuration files
├── logs/            # Execution logs
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Prerequisites

- Python 3.9+
- Docker & Docker Compose
- Apache Spark (or PySpark)
- Apache Airflow

## Getting Started

1. Set up virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. Launch services using Docker Compose:
   ```bash
   docker-compose up -d
   ```

## Architecture

```text
CSV Data Sources
       |
       v
Python Ingestion
       |
       v
PostgreSQL
(Raw Tables)
       |
       v
PySpark Transformation
       |
       v
Silver Layer
       |
       v
Data Quality Checks
       |
       v
Gold Layer
       |
       v
Analytics / BI