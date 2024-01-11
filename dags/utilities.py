import datetime

from google.cloud import storage

def get_date():
    """Get URL template for last month's trips data."""

    first_day_of_this_month = datetime.date.today().replace(day=1)
    last_month_year = first_day_of_this_month - datetime.timedelta(days=1)

    last_month = last_month_year.strftime("%m")
    last_year = last_month_year.strftime("%Y")

    return f"{last_year}-{last_month}.parquet"


def instantiate_google_client(project_id, bucket_name):
    """Instantiates a Google client and checks if bucket_name bucket exists."""
    storage_client = storage.Client(project=project_id)
    storage_client.create_bucket(bucket_name)

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
