import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

# Configuration
BUCKET_NAME = "module_04_98087_ny_taxi_2019_2020"
CREDENTIALS_FILE = "gcp-key.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

# Data sources
TAXI_TYPES = ["yellow", "green"]
YEARS = [2019, 2020]
MONTHS = [f"{i:02d}" for i in range(1, 13)]  # 01 to 12

DOWNLOAD_DIR = "."
CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
bucket = client.bucket(BUCKET_NAME)


def get_url(taxi_type, year, month):
    """Generate download URL for DataTalksClub taxi data."""
    return f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{year}-{month}.csv.gz"


def download_file(taxi_type, year, month):
    """Download a single file."""
    url = get_url(taxi_type, year, month)
    filename = f"{taxi_type}_tripdata_{year}-{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, filename)

    try:
        print(f"Downloading {filename}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {filename}")
        return file_path
    except Exception as e:
        print(f"Failed to download {filename}: {e}")
        return None


def create_bucket(bucket_name):
    """Create bucket if it doesn't exist."""
    try:
        bucket = client.get_bucket(bucket_name)
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(f"Bucket '{bucket_name}' exists. Proceeding...")
        else:
            print(f"Bucket '{bucket_name}' exists but not in your project.")
            sys.exit(1)
    except NotFound:
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        print(f"Bucket '{bucket_name}' exists but not accessible.")
        sys.exit(1)


def verify_gcs_upload(blob_name):
    """Verify file exists in GCS."""
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    """Upload file to GCS with retry logic."""
    if file_path is None:
        return
    
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {blob_name} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verified: {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {blob_name}: {e}")

        time.sleep(5)

    print(f"Giving up on {blob_name} after {max_retries} attempts.")


def download_wrapper(args):
    """Wrapper for ThreadPoolExecutor."""
    return download_file(*args)


if __name__ == "__main__":
    # Create bucket first
    create_bucket(BUCKET_NAME)

    # Generate all (taxi_type, year, month) combinations
    download_tasks = [
        (taxi_type, year, month)
        for taxi_type in TAXI_TYPES
        for year in YEARS
        for month in MONTHS
    ]

    print(f"Total files to process: {len(download_tasks)}")

    # Download all files
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_wrapper, download_tasks))

    # Upload all files
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, file_paths)

    print("All files processed.")