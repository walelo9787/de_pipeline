import os
import logging

from datetime import datetime as dt

import pyarrow.csv as pv
import pyarrow.parquet as pq

from google.cloud import storage

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from utilities import load_validate_set_config, get_date, instantiate_google_storage_client,\
    instantiate_google_bigquery_client, get_blob_uri

# global params
logging.basicConfig(level=logging.DEBUG)

# load config
params = load_validate_set_config()

# instantiate a google storage client
storage_client = instantiate_google_storage_client(params["gc_project_id"], params["gcs_bucket_name"])

# instantiate a google bigquery client
bigquery_client, bq_config = instantiate_google_bigquery_client()

# define DAG tasks
def download_files(trips_url_template):
    """Download trips and zones data."""
    trips_url = params["trips_url_prefix"] + trips_url_template
    zones_url = params["zones_url"]
    logging.debug(f"Downloading trips data from : {trips_url} \n and zones data from : {zones_url}")
    bash_command = f'curl -sSL {trips_url} {zones_url} -o {params["trips_output"]} -o {params["zones_output"]}'
    os.system(bash_command)

def verify_downloading(trips_path, zones_path):
    """Verify downloaded files are saved under the expected paths."""
    logging.debug(f"Checking trips data under {trips_path} \n and zones data under {zones_path}")
    trips_found = os.path.exists(trips_path)
    if not trips_found:
        logging.debug("Trips data not found. Stopping DAG.")
        return False
    zones_found = os.path.exists(zones_path)
    if not zones_found:
        logging.debug("Zones data not found. Stopping DAG.")
        return False

    return True

def csv_to_parquet(zones_path):
    """Convert zones data from csv to parquet and deleting csv source once done."""
    parquet_path = params["zones_parquet_output"]
    table = pv.read_csv(zones_path)
    pq.write_table(table, parquet_path)

    os.system(f"rm {zones_path}")

    parquet_zones_found = os.path.exists(parquet_path)
    if not parquet_zones_found:
        logging.debug("Zones data conversion failed. Stopping DAG")
        return False
    csv_zones_not_deleted = os.path.exists(zones_path)
    if csv_zones_not_deleted:
        logging.warn("Zones csv data not deleted.")

    return True

def ingest_to_gcs(bucket_name, trips_path, zones_path):
    """Ingest parquet files to Google Cloud Storage bucket. Once uploaded, delete source files."""

    # connect to bucket
    try:
        bucket = storage_client.get_bucket(bucket_name)
        logging.debug(f"Connection to bucket {bucket_name} succeeded.")
    except Exception as e:
        logging.warn(f"Connection to bucket {bucket_name} failed and raised the following\n {e}")
        return False

    # make sure parquet files exist
    assert os.path.exists(trips_path), f"Trips parquet file : {trips_path} not found."
    assert os.path.exists(zones_path), f"Zones parquet file : {zones_path} not found."

    # create blob for the to-be-uploaded files
    gcs_trips_path = os.path.join(params["base_folder"], trips_path.split("/")[-1])
    gcs_zones_path = os.path.join(params["base_folder"], zones_path.split("/")[-1])
    trips_blob = bucket.blob(gcs_trips_path)
    zones_blob = bucket.blob(gcs_zones_path)
    assert trips_blob is not None, "Trips blob is not created. Failing..."
    assert zones_blob is not None, "Zones blob is not created. Failing..."

    # WORKAROUND to prevent time out for relatively large files
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024

    # upload files
    trips_blob.upload_from_filename(trips_path, if_generation_match=0)
    zones_blob.upload_from_filename(zones_path, if_generation_match=0)

    # deleting source files
    os.system(f"rm {zones_path} {trips_path}")

    return True

def ingest_to_bq():
    """Ingest parquet files from storage to BigQuery."""

    # ingest all parquets in the base folder
    parquets = [
        os.path.join(params["gcs_bucket_name"], params["base_folder"], params["trips_output"].split("/")[-1]),
        os.path.join(params["gcs_bucket_name"], params["base_folder"], params["zones_parquet_output"].split("/")[-1])
    ]
    for blob in parquets:
        table_name = blob.split("/")[-1].split(".")[0]
        uri = get_blob_uri(blob)
        table_id = f"{params['gc_project_id']}.{params['gc_dataset_name']}.{table_name}"
        load_job = bigquery_client.load_table_from_uri(
            uri, table_id, job_config=bq_config
        )
        load_job.result()


# define dag
workflow = DAG(start_date=dt(2024, 1, 2), dag_id="Ingestion_DAG", schedule_interval="0 7 2 * *")

with workflow:

    download_files_task = PythonOperator(
        task_id="download_files",
        python_callable=download_files,
        op_kwargs=dict(
            # Make sure that for the provided date, last month's data is uploaded to the website
            # Check https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page for more info.
            trips_url_template=get_date(dt(2023, 10, 2))
        )
    )

    verify_downloading_task = ShortCircuitOperator(
        task_id="verify_downloading",
        provide_context=True,
        python_callable=verify_downloading,
        op_kwargs=dict(
            trips_path=params["trips_output"],
            zones_path=params["zones_output"]
        )
    )

    csv_to_parquet_task = ShortCircuitOperator(
        task_id="csv_to_parquet",
        provide_context=True,
        python_callable=csv_to_parquet,
        op_kwargs=dict(
            zones_path=params["zones_output"]
        )
    )

    ingest_to_gcs_task = ShortCircuitOperator(
        task_id="ingest_to_gcs",
        provide_context=True,
        python_callable=ingest_to_gcs,
        op_kwargs=dict(
            bucket_name=params["gcs_bucket_name"],
            trips_path=params["trips_output"],
            zones_path=params["zones_parquet_output"]
        )
    )

    ingest_trips_to_bq_task = BigQueryCreateExternalTableOperator(
        task_id="ingest_trips_to_bq",
        table_resource={
            "tableReference": {
                "projectId": params["gc_project_id"],
                "datasetId": params["gc_dataset_name"],
                "tableId": "external_table_trips"
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [get_blob_uri(os.path.join(params["gcs_bucket_name"], params["base_folder"], params["trips_output"].split("/")[-1]))]
            }
        }

    )

    ingest_zones_to_bq_task = BigQueryCreateExternalTableOperator(
        task_id="ingest_zones_to_bq",
        table_resource={
            "tableReference": {
                "projectId": params["gc_project_id"],
                "datasetId": params["gc_dataset_name"],
                "tableId": "external_table_zones"
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [get_blob_uri(os.path.join(params["gcs_bucket_name"], params["base_folder"], params["zones_parquet_output"].split("/")[-1]))]
            }
        }

    )

    download_files_task >> verify_downloading_task >> csv_to_parquet_task >> ingest_to_gcs_task >> ingest_trips_to_bq_task >> ingest_zones_to_bq_task