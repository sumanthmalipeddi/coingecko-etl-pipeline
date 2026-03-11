import json
import requests
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


default_args = {
    'owner' : 'sumanth',
    'depends_on_past' : False
}

GCP_PROJECT = 'learn-airflow-489817'
GCS_BUCKET = 'crypto-exchange-pipeline-sumanth'
BIGQUERY_DATASET = 'crypto_db'
BIGQUERY_TABLE = 'tbl_crypto'

BQ_SCHEMA = [
    {'name' : 'id', 'type' : 'STRING', 'mode' : 'REQUIRED'},
    {'name' : 'symbol', 'type' : 'STRING', 'mode' : 'REQUIRED'},
    {'name' : 'name', 'type' : 'STRING', 'mode' : 'REQUIRED'},
    {'name' : 'current_price', 'type' : 'FLOAT', 'mode' : 'NULLABLE'},
    {'name' : 'market_cap', 'type' : 'FLOAT', 'mode' : 'NULLABLE'},
    {'name' : 'total_volume', 'type' : 'FLOAT', 'mode' : 'NULLABLE'},
    {'name' : 'last_updated', 'type' : 'TIMESTAMP', 'mode' : 'NULLABLE'},
    {'name' : 'timestamp', 'type' : 'TIMESTAMP', 'mode' : 'REQUIRED'},
]

def _fetch_and_upload_raw(**kwargs):
    """Fetch data from CoinGecko API and upload directly to GCS."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency' : 'usd',
        'order' : 'market_cap_desc',
        'per_page' : 10,
        'page' : 1,
        'sparkline' : False
    }
    response = requests.get(url, params=params)
    data = response.json()
    raw_json = json.dumps(data)

    ts = kwargs['ts_nodash']
    gcs_path = f"raw_data/crypto_raw_data_{ts}.json"

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=gcs_path,
        data=raw_json,
        mime_type='application/json'
    )

    # Push GCS path to XCom so next task can find the file
    kwargs['ti'].xcom_push(key='raw_gcs_path', value=gcs_path)


def _transform_and_upload(**kwargs):
    """Download raw data from GCS, transform it, upload transformed CSV to GCS."""
    ti = kwargs['ti']
    raw_gcs_path = ti.xcom_pull(task_ids='fetch_and_upload_raw', key='raw_gcs_path')

    # Download raw data from GCS
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    raw_data = gcs_hook.download(
        bucket_name=GCS_BUCKET,
        object_name=raw_gcs_path
    )
    data = json.loads(raw_data)

    # Transform
    transform_data = []
    for item in data:
        transform_data.append({
            'id' : item['id'],
            'symbol' : item['symbol'],
            'name' : item['name'],
            'current_price' : item['current_price'],
            'market_cap' : item['market_cap'],
            'total_volume' : item['total_volume'],
            'last_updated' : item['last_updated'],
            'timestamp' : datetime.utcnow().isoformat()
        })

    df = pd.DataFrame(transform_data)
    csv_data = df.to_csv(index=False)

    # Upload transformed CSV to GCS
    ts = kwargs['ts_nodash']
    transformed_gcs_path = f"transformed_data/crypto_transformed_data_{ts}.csv"

    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=transformed_gcs_path,
        data=csv_data,
        mime_type='text/csv'
    )

    ti.xcom_push(key='transformed_gcs_path', value=transformed_gcs_path)


dag = DAG(
    dag_id = 'crypto_exchange_pipeline_composer_v2',
    default_args= default_args,
    description= "Fetch data from coingecko api (Composer v2 - no /tmp/ sharing)",
    schedule = timedelta(minutes=10),
    start_date= datetime(2026,3,11),
    catchup = False
)

# Step 1: Fetch from API + upload raw to GCS (single task, no /tmp/ needed)
fetch_and_upload_raw = PythonOperator(
    task_id = "fetch_and_upload_raw",
    python_callable = _fetch_and_upload_raw,
    dag = dag
)

# Step 2: Download raw from GCS, transform, upload transformed CSV to GCS
transform_and_upload = PythonOperator(
    task_id = 'transform_and_upload',
    python_callable = _transform_and_upload,
    dag = dag,
)

# Step 3: Create BigQuery dataset
create_bigquery_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id = 'create_bigquery_dataset',
    dataset_id = BIGQUERY_DATASET,
    gcp_conn_id= 'google_cloud_default',
    dag = dag
)

# Step 4: Create BigQuery table
create_bigquery_table_task = BigQueryInsertJobOperator(
    task_id = 'create_bigquery_table',
    configuration = {
        "query": {
            "query": f"""
                CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}` (
                    id STRING NOT NULL,
                    symbol STRING NOT NULL,
                    name STRING NOT NULL,
                    current_price FLOAT64,
                    market_cap FLOAT64,
                    total_volume FLOAT64,
                    last_updated TIMESTAMP,
                    timestamp TIMESTAMP NOT NULL
                )
            """,
            "useLegacySql": False,
        }
    },
    gcp_conn_id= 'google_cloud_default',
    dag = dag
)

# Step 5: Load transformed CSV from GCS into BigQuery
load_to_bigquery = GCSToBigQueryOperator(
    task_id = 'load_to_bigquery',
    bucket= GCS_BUCKET,
    source_objects= ["transformed_data/crypto_transformed_data_{{ ts_nodash }}.csv"],
    destination_project_dataset_table= f'{GCP_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}',
    source_format= 'csv',
    schema_fields= BQ_SCHEMA,
    write_disposition='WRITE_APPEND',
    skip_leading_rows= 1,
    gcp_conn_id='google_cloud_default',
    dag = dag
)

fetch_and_upload_raw >> transform_and_upload >> create_bigquery_dataset_task >> create_bigquery_table_task >> load_to_bigquery
