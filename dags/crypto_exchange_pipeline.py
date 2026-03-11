import json
import requests
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator


default_args = {
    'owner' : 'sumanth',
    'depends_on_past' : False
}

GCP_PROJECT = 'learn-airflow-489817'
GCS_BUCKET = 'crypto-exchange-pipeline-sumanth'
GCS_RAW_DATA_PATH = 'raw_data/crypto_raw_data'
GCS_TRANSFORMED_PATH = 'transformed_data/crypto_transformed_data'
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

def _fetch_data_from_api():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency' : 'usd',
        'order' : 'market_cap_desc',
        'per_page' : 10,
        'page' : 1,
        'sparkline' : False
    }
    response = requests.get(url, params= params)

    data = response.json()
    
    with open("/tmp/crypto_data.json", 'w') as f:
        json.dump(data, f)

def _transform_data():
    with open('/tmp/crypto_data.json','r') as f:
        data = json.load(f)

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
        df.to_csv('/tmp/transformed_data.csv', index = False)
        

dag = DAG(
    dag_id = 'crypto_exchange_pipeline',
    default_args= default_args,
    description= "Fetch data from coingecko api",
    schedule = timedelta(minutes=10),
    start_date= datetime(2026,3,11),
    catchup = False
)

#fetch data from crypto api
fetch_data = PythonOperator(
    task_id = "fetch_data_from_api",
    python_callable = _fetch_data_from_api,
    dag = dag
)

#create GCS bucket
create_bucket_task = GCSCreateBucketOperator(
    task_id ="create_bucket",
    bucket_name= GCS_BUCKET,
    storage_class= 'MULTI_REGIONAL',
    location = 'US',
    gcp_conn_id= 'google_cloud_default',
    dag = dag
    )

#upload raw data to GCS
upload_raw_data_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id = "upload_raw_data_to_gcs",
    src= '/tmp/crypto_data.json',
    dst= GCS_RAW_DATA_PATH + "_{{ ts_nodash }}.json",
    bucket= GCS_BUCKET,
    gcp_conn_id= 'google_cloud_default',
    dag = dag
)

transform_data_task = PythonOperator(
    task_id = 'transformed_data',
    python_callable = _transform_data,
    dag = dag,
)

#upload transformed data to GCS
upload_transformed_data_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id = "upload_transformed_data_to_gcs",
    src= '/tmp/transformed_data.csv',
    dst= GCS_TRANSFORMED_PATH + "_{{ ts_nodash }}.csv",
    bucket= GCS_BUCKET,
    gcp_conn_id= 'google_cloud_default',
    dag = dag
)

#create big query dataset
create_bigquery_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id = 'create_bigquery_dataset',
    dataset_id = BIGQUERY_DATASET,
    gcp_conn_id= 'google_cloud_default',
    dag = dag
)

#create big query table
create_bigquery_table_task = BigQueryCreateEmptyTableOperator(
    task_id = 'create_bigquery_table',
    dataset_id = BIGQUERY_DATASET,
    table_id = BIGQUERY_TABLE,
    schema_fields = BQ_SCHEMA,
    gcp_conn_id= 'google_cloud_default',
    dag = dag
)


#load data to bigquery
load_to_bigquery = GCSToBigQueryOperator(
    task_id = 'load_to_bigquery',
     bucket= GCS_BUCKET,
     source_objects= [GCS_TRANSFORMED_PATH + "_{{ ts_nodash }}.csv"],
     destination_project_dataset_table= f'{GCP_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}',
     source_format= 'csv',
     schema_fields= BQ_SCHEMA,
     write_disposition='WRITE_APPEND',
     skip_leading_rows= 1,
     gcp_conn_id='google_cloud_default',
     dag = dag
)


fetch_data >> create_bucket_task >> upload_raw_data_to_gcs_task
upload_raw_data_to_gcs_task >> transform_data_task >> upload_transformed_data_to_gcs_task
upload_transformed_data_to_gcs_task >> create_bigquery_dataset_task >> create_bigquery_table_task
create_bigquery_table_task >> load_to_bigquery
