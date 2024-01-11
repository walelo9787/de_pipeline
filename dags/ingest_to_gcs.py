import os
import logging

from datetime import datetime as dt

import pyarrow.csv as pv
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from utilities import get_date, instantiate_google_client

# global params
logging.basicConfig(level=logging.DEBUG)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

params = {
    "trips_url_prefix" : "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_",
    "trips_output" : f"{AIRFLOW_HOME}/trips.parquet",
    "zones_url" : "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv",
    "zones_output" : f"{AIRFLOW_HOME}/zones.csv",
    "gc_credentials_filename": "gc-creds.json",
    "gc_project_id": "white-defender-410709",
    "gcs_bucket_name": "whatever"
}

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = params["gc_credentials_filename"]
client = instantiate_google_client(params["gc_project_id"], params["gcs_bucket_name"])

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
    parquet_path = zones_path.replace('csv', 'parquet')
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

def ingest_to_gcs():
    """Ingest parquet files to Google Cloud Storage bucket."""
    logging.debug("Ingesting...")

# define dag
workflow = DAG(start_date=dt(2024, 1, 2), dag_id="IngestionDAG", schedule_interval="0 7 2 * *")

with workflow:

    download_files_task = PythonOperator(
        task_id="download_files",
        python_callable=download_files,
        op_kwargs=dict(
            trips_url_template=get_date()
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

    ingest_to_gcs_task = PythonOperator(
        task_id="ingest_to_gcs",
        python_callable=ingest_to_gcs,
        op_kwargs={}
    )


    download_files_task >> verify_downloading_task >> csv_to_parquet_task >> ingest_to_gcs_task