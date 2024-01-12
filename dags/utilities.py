import datetime
import json
import os
import logging

from google.cloud import storage
from google.cloud import bigquery

logging.basicConfig(level=logging.DEBUG)

def load_validate_set_config():
    """Load, validate and set defaults for the config file."""

    config_path = "config/params.json"
    # check config file exists
    logging.debug(f"Checking config file under path {config_path}")
    config_found = os.path.exists(config_path)

    # fail if config file is missing
    assert config_found, "Config file not found under specified path. Please make sure to provide a configuration file."

    logging.debug("Configuration file found. Proceeding to validate config...")

    config_file = open(config_path)
    params = json.load(config_file)
    config_file.close()

    params["gc_project_id"] = os.getenv("GC_PROJECT_ID")
    params["gcs_bucket_name"] = os.getenv("GCS_BUCKET_NAME")
    params["gc_dataset_name"] = os.getenv("GC_DATASET_NAME")

    # Google cloud params should be set as environment vars
    assert params["gc_project_id"], "Google cloud project id is not set."
    assert params["gcs_bucket_name"], "Google cloud bucket name is not set."
    assert params["gc_dataset_name"], "Google cloud bigQuery dataset name is not set."

    # default other params
    AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

    params["trips_url_prefix"] = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
    params["zones_url"] = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    params["trips_output"] = f"{AIRFLOW_HOME}/trips.parquet"
    params["zones_output"] = f"{AIRFLOW_HOME}/zones.csv"
    params["zones_parquet_output"] = f"{AIRFLOW_HOME}/zones.parquet"
    params["base_folder"] = "parquets"
    params["gc_credentials_filename"] = "gc-creds.json"

    # set google env variable for authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = params["gc_credentials_filename"]

    return params

def get_date(today):
    """Get URL template for last month's trips data."""

    first_day_of_this_month = today.replace(day=1)
    last_month_year = first_day_of_this_month - datetime.timedelta(days=1)

    last_month = last_month_year.strftime("%m")
    last_year = last_month_year.strftime("%Y")

    return f"{last_year}-{last_month}.parquet"

def instantiate_google_storage_client(project_id, bucket_name):
    """Instantiates a Google client and checks if {bucket_name} bucket exists."""
    storage_client = storage.Client(project=project_id)

    # check if bucket_name exists
    bucket_name_exists = storage_client.bucket(bucket_name).exists()
    if not bucket_name_exists:
        logging.debug(f"Bucket {bucket_name} not found.")

    # list files in the bucket just for information
    logging.debug(f"Listing files in {bucket_name}")
    blobs = storage_client.list_blobs(bucket_name)
    for blob in blobs:
        logging.debug(f"Found {blob.name}.")

    return storage_client

def instantiate_google_bigquery_client():
    """Instantiates a Google BigQuery client."""
    bigquery_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
    )
    return bigquery_client, job_config

def get_blob_uri(blob):
    """Returns blob URI to be used for loading it to BigQuery."""
    return 'gs://' + blob
