# Design Document: Crypto Data Pipeline

## Overview
An ETL pipeline that extracts cryptocurrency market data from the CoinGecko API, transforms it into a structured format, and loads it into Google BigQuery via Google Cloud Storage. The pipeline is orchestrated by Apache Airflow and runs on Google Cloud Composer.

## Data Source
- **API**: CoinGecko `/coins/markets` endpoint (free tier, no API key)
- **Data**: Top 10 cryptocurrencies by market cap
- **Frequency**: Every 10 minutes
- **Fields**: id, symbol, name, current_price, market_cap, total_volume, last_updated

## Architecture Diagram

```
┌──────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌──────────────┐
│  CoinGecko   │────>│  Airflow Task 1  │────>│  GCS (Raw JSON) │     │              │
│  API         │     │  fetch & upload   │     │                 │     │   BigQuery   │
└──────────────┘     └──────────────────┘     └────────┬────────┘     │              │
                                                       │              │  crypto_db.  │
                                                       v              │  tbl_crypto  │
                     ┌──────────────────┐     ┌─────────────────┐     │              │
                     │  Airflow Task 2  │────>│  GCS (CSV)      │────>│              │
                     │  transform &     │     │                 │     │              │
                     │  upload          │     └─────────────────┘     └──────────────┘
                     └──────────────────┘
```

## Pipeline Tasks

### Task 1: fetch_and_upload_raw
- Calls CoinGecko API with parameters: `vs_currency=usd`, `per_page=10`, `order=market_cap_desc`
- Serializes response to JSON string
- Uploads directly to GCS using `GCSHook.upload(data=...)` — no local file I/O
- Pushes GCS object path to XCom for downstream tasks

### Task 2: transform_and_upload
- Downloads raw JSON from GCS using `GCSHook.download()`
- Extracts relevant fields: id, symbol, name, current_price, market_cap, total_volume, last_updated
- Adds `timestamp` field (UTC extraction time)
- Converts to pandas DataFrame, exports as CSV string
- Uploads CSV directly to GCS — no local file I/O

### Task 3: create_bigquery_dataset
- Uses `BigQueryCreateEmptyDatasetOperator`
- Creates `crypto_db` dataset (idempotent — skips if exists)

### Task 4: create_bigquery_table
- Uses `BigQueryInsertJobOperator` with `CREATE TABLE IF NOT EXISTS` SQL
- Chosen over `BigQueryCreateEmptyTableOperator` for Composer compatibility

### Task 5: load_to_bigquery
- Uses `GCSToBigQueryOperator`
- Loads CSV from GCS into BigQuery with `WRITE_APPEND` disposition
- Skips CSV header row

## Storage Layout

### Google Cloud Storage
```
gs://crypto-exchange-pipeline-sumanth/
├── raw_data/
│   └── crypto_raw_data_<ts_nodash>.json    # Raw API response
└── transformed_data/
    └── crypto_transformed_data_<ts_nodash>.csv  # Cleaned, structured data
```

### BigQuery
- **Project**: `learn-airflow-489817`
- **Dataset**: `crypto_db`
- **Table**: `tbl_crypto` (append-only)

## Environment Setup

### Local Development
- **Airflow 2.10.5** running in Docker Compose
- **CeleryExecutor** with PostgreSQL (metadata DB) and Redis (message broker)
- Services: webserver, scheduler, worker, triggerer
- DAG file: `crypto_exchange_pipeline.py` — uses `/tmp/` for inter-task file sharing (works because all tasks share the same Docker volume)

### Google Cloud Composer
- **Managed Airflow** on Google Kubernetes Engine
- DAG file: `crypto_exchange_pipeline_composer_v2.py` — uses GCSHook for all data transfer
- Each task runs in a separate Kubernetes pod — `/tmp/` is NOT shared

## Design Decisions

### 1. Why GCSHook instead of LocalFilesystemToGCSOperator?
In Composer, each Airflow task runs in its own Kubernetes pod with isolated filesystem. The `LocalFilesystemToGCSOperator` requires a file on the local filesystem, but files written by a previous task don't exist on the next task's pod. Using `GCSHook` within a PythonOperator allows us to upload data directly from memory without writing to disk.

### 2. Why separate local and Composer DAGs?
The local DAG (`crypto_exchange_pipeline.py`) uses operators like `LocalFilesystemToGCSOperator` and `BigQueryCreateEmptyTableOperator` which work fine in Docker but break on Composer. Rather than maintaining one complex DAG with conditional logic, separate files are simpler and clearer.

### 3. Why BigQueryInsertJobOperator for table creation?
`BigQueryCreateEmptyTableOperator` is not available in all versions of the Google Cloud provider package. `BigQueryInsertJobOperator` with a `CREATE TABLE IF NOT EXISTS` SQL query works across all versions and is the recommended approach for Composer.

### 4. Why XCom for passing GCS paths?
XCom passes small metadata (like file paths) between tasks. The actual data goes through GCS. This keeps XCom lightweight while ensuring tasks can find the right files.

### 5. Why WRITE_APPEND?
Each pipeline run appends new rows to the BigQuery table, building a time-series of crypto prices. This enables historical analysis of price trends over time.

## Limitations
- **No error handling/retries** — tasks have no retry configuration
- **No data validation** — no null checks or schema validation before BigQuery load
- **Duplicate rows** — if the DAG reruns for the same interval, data is appended again
- **CoinGecko rate limits** — free tier has rate limits that could cause failures under heavy use
- **Hardcoded configuration** — project ID, bucket name, and table names are hardcoded in the DAG files
