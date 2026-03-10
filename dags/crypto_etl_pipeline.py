"""
Crypto Data Pipeline
====================
ETL pipeline that fetches cryptocurrency market data from the CoinGecko API,
transforms it, and stores it in Google Cloud Storage.

Pipeline flow: extract → upload_raw → read → transform → store → archive
"""

import logging
import json
import pandas as pd
import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONN_ID = "google_cloud_default"
OUTPUT_FORMAT = "csv"

# CoinGecko free API base URL (no API key needed for basic endpoints)
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"

# Top coins to track
COINS = "bitcoin,ethereum,solana,cardano,polkadot,chainlink,avalanche-2,polygon-pos,cosmos,near"

default_args = {
    "owner": "crypto-pipeline",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _get_config():
    """Load pipeline config from Airflow Variables."""
    return {
        "bucket_name": Variable.get("gcs_bucket_name", default_var="crypto-pipeline-bucket"),
    }


# ===========================================================================
# STEP 1: EXTRACT — Fetch crypto market data from CoinGecko
# ===========================================================================

def _extract_data(**kwargs):
    """Extract cryptocurrency market data from CoinGecko API."""
    config = _get_config()

    url = f"{COINGECKO_BASE_URL}/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": COINS,
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "1h,24h,7d",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    raw_data = response.json()

    filename = "crypto_raw_" + datetime.now().strftime("%Y%m%d%H%M%S") + ".json"
    tmp_path = f"/tmp/{filename}"

    with open(tmp_path, "w") as f:
        json.dump(raw_data, f)

    logger.info("Extracted %d coins, saved to %s", len(raw_data), tmp_path)
    kwargs["ti"].xcom_push(key="raw_filename", value=filename)
    kwargs["ti"].xcom_push(key="raw_tmp_path", value=tmp_path)


# ===========================================================================
# STEP 2: UPLOAD RAW — Store raw data in GCS (landing zone)
# ===========================================================================

def _upload_raw_to_gcs(**kwargs):
    """Upload raw extracted data to Google Cloud Storage."""
    ti = kwargs["ti"]
    config = _get_config()
    filename = ti.xcom_pull(task_ids="extract_data", key="raw_filename")
    tmp_path = ti.xcom_pull(task_ids="extract_data", key="raw_tmp_path")

    gcs_hook = GCSHook(gcp_conn_id=CONN_ID)
    gcs_hook.upload(
        bucket_name=config["bucket_name"],
        object_name=f"raw_data/to_processed/{filename}",
        filename=tmp_path,
    )
    logger.info("Uploaded %s to gs://%s/raw_data/to_processed/", filename, config["bucket_name"])


# ===========================================================================
# STEP 3: READ — Load raw data from GCS for transformation
# ===========================================================================

def _read_from_gcs(**kwargs):
    """Read all raw files from GCS to_processed folder."""
    config = _get_config()
    gcs_hook = GCSHook(gcp_conn_id=CONN_ID)
    prefix = "raw_data/to_processed/"
    blobs = gcs_hook.list(bucket_name=config["bucket_name"], prefix=prefix)
    blobs = [b for b in blobs if not b.endswith("/")]

    if not blobs:
        raise ValueError(f"No files found in gs://{config['bucket_name']}/{prefix}")

    all_data = []
    for blob in blobs:
        data = gcs_hook.download(bucket_name=config["bucket_name"], object_name=blob)
        all_data.append(json.loads(data))

    tmp_path = "/tmp/consolidated_raw.json"
    with open(tmp_path, "w") as f:
        json.dump(all_data, f)

    logger.info("Read %d raw files from GCS, consolidated to %s", len(all_data), tmp_path)
    kwargs["ti"].xcom_push(key="data_path", value=tmp_path)


# ===========================================================================
# STEP 4: TRANSFORM — Clean and structure the crypto data
# ===========================================================================

def _transform_data(**kwargs):
    """Transform raw crypto market data into a clean dataset."""
    data_path = kwargs["ti"].xcom_pull(task_ids="read_from_gcs", key="data_path")
    with open(data_path, "r") as f:
        all_data = json.load(f)

    records = []
    for batch in all_data:
        for coin in batch:
            records.append({
                "id": coin["id"],
                "symbol": coin["symbol"],
                "name": coin["name"],
                "current_price_usd": coin["current_price"],
                "market_cap": coin["market_cap"],
                "market_cap_rank": coin["market_cap_rank"],
                "total_volume_24h": coin["total_volume"],
                "price_change_pct_24h": coin["price_change_percentage_24h"],
                "price_change_pct_7d": coin.get("price_change_percentage_7d_in_currency"),
                "circulating_supply": coin["circulating_supply"],
                "total_supply": coin["total_supply"],
                "ath": coin["ath"],
                "ath_date": coin["ath_date"],
                "last_updated": coin["last_updated"],
                "extracted_at": datetime.now().isoformat(),
            })

    df = pd.DataFrame(records)

    # Deduplicate by coin id
    df = df.drop_duplicates(subset=["id"])

    # Validate
    _validate(df, pk_column="id", dataset_name="crypto_market_data")

    if OUTPUT_FORMAT == "parquet":
        tmp_path = "/tmp/transformed_crypto.parquet"
        df.to_parquet(tmp_path, index=False, engine="pyarrow")
    else:
        tmp_path = "/tmp/transformed_crypto.csv"
        df.to_csv(tmp_path, index=False)

    logger.info("Transformed %d records, saved to %s", len(df), tmp_path)
    kwargs["ti"].xcom_push(key="transformed_path", value=tmp_path)


def _validate(df, pk_column, dataset_name):
    """Data quality checks."""
    if len(df) == 0:
        raise ValueError(f"{dataset_name}: empty dataframe - 0 rows")

    null_count = df[pk_column].isnull().sum()
    if null_count > 0:
        raise ValueError(f"{dataset_name}: {null_count} null values in {pk_column}")

    dup_count = df[pk_column].duplicated().sum()
    if dup_count > 0:
        raise ValueError(f"{dataset_name}: {dup_count} duplicate {pk_column} values")

    logger.info("%s: validated %d rows, 0 nulls, 0 duplicates", dataset_name, len(df))


# ===========================================================================
# STEP 5: STORE — Upload transformed data to GCS
# ===========================================================================

def _store_to_gcs(**kwargs):
    """Upload transformed file to GCS."""
    config = _get_config()
    file_path = kwargs["ti"].xcom_pull(task_ids="transform_data", key="transformed_path")
    gcs_hook = GCSHook(gcp_conn_id=CONN_ID)
    ts = kwargs["ts_nodash"]

    ext = "parquet" if OUTPUT_FORMAT == "parquet" else "csv"
    gcs_key = f"transformed_data/output/crypto_market_{ts}.{ext}"

    gcs_hook.upload(
        bucket_name=config["bucket_name"],
        object_name=gcs_key,
        filename=file_path,
    )
    logger.info("Stored transformed data to gs://%s/%s", config["bucket_name"], gcs_key)


# ===========================================================================
# STEP 6: ARCHIVE — Move raw files so they don't get re-processed
# ===========================================================================

def _archive_processed(**kwargs):
    """Move raw files from to_processed/ to processed/ after successful transform."""
    config = _get_config()
    gcs_hook = GCSHook(gcp_conn_id=CONN_ID)
    prefix = "raw_data/to_processed/"
    target_prefix = "raw_data/processed/"
    blobs = gcs_hook.list(bucket_name=config["bucket_name"], prefix=prefix)
    blobs = [b for b in blobs if not b.endswith("/")]

    moved = 0
    for blob in blobs:
        if blob.endswith(".json"):
            new_name = blob.replace(prefix, target_prefix)
            gcs_hook.copy(
                source_bucket=config["bucket_name"],
                source_object=blob,
                destination_bucket=config["bucket_name"],
                destination_object=new_name,
            )
            gcs_hook.delete(bucket_name=config["bucket_name"], object_name=blob)
            moved += 1
    logger.info("Archived %d raw files from to_processed -> processed", moved)


# ===========================================================================
# DAG DEFINITION
# ===========================================================================

dag = DAG(
    dag_id="crypto_etl_pipeline",
    default_args=default_args,
    description="Crypto market data pipeline: CoinGecko API → GCS (CSV/Parquet)",
    schedule=timedelta(hours=1),  # Run hourly to track crypto prices
    catchup=False,
)

extract = PythonOperator(task_id="extract_data", python_callable=_extract_data, dag=dag)
upload_raw = PythonOperator(task_id="upload_raw_to_gcs", python_callable=_upload_raw_to_gcs, dag=dag)
read = PythonOperator(task_id="read_from_gcs", python_callable=_read_from_gcs, dag=dag)
transform = PythonOperator(task_id="transform_data", python_callable=_transform_data, dag=dag)
store = PythonOperator(task_id="store_to_gcs", python_callable=_store_to_gcs, dag=dag)
archive = PythonOperator(task_id="archive_processed", python_callable=_archive_processed, dag=dag)

# Pipeline flow: extract → upload → read → transform → store → archive
extract >> upload_raw >> read >> transform >> store >> archive
