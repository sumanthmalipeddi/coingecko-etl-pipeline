# Crypto Data Pipeline

ETL pipeline that fetches cryptocurrency market data from the CoinGecko API, transforms it, and stores it in Google Cloud Storage using Apache Airflow.

## Architecture

```
CoinGecko API → Airflow → Google Cloud Storage
```

### Pipeline Steps
1. **Extract** — Fetch market data (price, volume, market cap) from CoinGecko
2. **Upload Raw** — Store raw JSON in GCS landing zone (`raw_data/to_processed/`)
3. **Read** — Consolidate raw files from GCS
4. **Transform** — Clean, deduplicate, validate, and structure data
5. **Store** — Upload transformed CSV/Parquet to GCS (`transformed_data/output/`)
6. **Archive** — Move raw files to `raw_data/processed/`

### GCS Bucket Layout
```
your-bucket/
├── raw_data/
│   ├── to_processed/    ← raw JSON lands here
│   └── processed/       ← archived after transform
└── transformed_data/
    └── output/          ← clean CSV/Parquet files
```

## Tech Stack
- **Apache Airflow 3.x** — orchestration
- **Docker Compose** — local infrastructure (PostgreSQL, Redis, Celery)
- **Google Cloud Storage** — data lake
- **CoinGecko API** — crypto market data (free, no API key needed)
- **pandas** — data transformation

## Getting Started

1. Clone and enter the repo
2. Copy env file: `cp .env.example .env` and fill in GCP settings
3. Build: `docker-compose build`
4. Start: `docker-compose up -d`
5. Open http://localhost:8080 (login: `airflow` / `airflow`)
6. Set up a Google Cloud connection in Airflow UI (Admin → Connections)
7. Unpause the `crypto_etl_pipeline` DAG

## GCP Connection Setup

In the Airflow UI, go to **Admin → Connections** and create:
- **Connection Id**: `google_cloud_default`
- **Connection Type**: Google Cloud
- **Keyfile JSON** or **Keyfile Path**: your service account credentials
