import argparse
from datetime import datetime
import requests
from pathlib import Path
import pyarrow.parquet as pq 
import boto3
from botocore.exceptions import ClientError
import logging
import sys


logger = logging.getLogger(__name__)

TAXI_FILE_PREFIX = {
    "yellow": "yellow_tripdata",
    "green": "green_tripdata",
    "fhv": "fhv_tripdata",
    "hvfhv": "fhvhv_tripdata",
}

# Note: Add checksum for source data check

def parse_arguments():
    parser = argparse.ArgumentParser(description="Download NYC TLC data")

    parser.add_argument(
        "--taxi-type",
        required=True,
        choices=["yellow", "green", "fhv", "hvfhv"],
        help="Type of taxi data to download (yellow, green, fhv, hvfhv)"
    ) 
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM format"
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM format"
    )
    parser.add_argument(
        "--bucket-name",
        required=True,
        help="Name of the S3 bucket to upload data to"
    )
    parser.add_argument(
        "source_system",
        help="Source system name"
    )

    parser.add_argument(
        "phase_name",
        help="Pipeline phase name"
    )
    return parser.parse_args()

def generate_months(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m")
    end = datetime.strptime(end_date, "%Y-%m") 

    months = []

    current = start

    while current<=end:
        months.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = current.replace(year=current.year+1, month=1)
        else: 
            current = current.replace(month=current.month+1)
    return months

def build_tlc_url(taxi_type,year_month):
    file_prefix = TAXI_FILE_PREFIX[taxi_type]
    filename = f"{file_prefix}_{year_month}.parquet"
    url = ("https://d37ci6vzurychx.cloudfront.net/"f"trip-data/{filename}")
    return url

def download_file(url, local_path):
    local_path = Path(local_path) 
    local_path.parent.mkdir(parents=True, exist_ok=True)  # Create directories if they don't exist

    logger.info("Downloading source file from %s to %s", url, local_path)
    response = requests.get(url, stream=True, timeout=60)  # Set a timeout for the request
    response.raise_for_status()  # Raise an error for bad responses

    bytes_downloaded = 0
    with open(local_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size = 1024*1024):  # Download in chunks of 1MB
            if chunk:  # Filter out keep-alive new chunks
                file.write(chunk)
                bytes_downloaded += len(chunk)

    logger.info("Downloaded %d bytes to %s", bytes_downloaded, local_path)

def validate_download(local_path):
    local_path = Path(local_path)

    if not local_path.exists():
        raise FileNotFoundError(
            f"Downloaded file not found: {local_path}"
        )

    if local_path.stat().st_size == 0:
        raise ValueError(
            f"Downloaded file is empty: {local_path}"
        )

    logger.info(
        "Download validation passed: %s (%d bytes)",
        local_path,
        local_path.stat().st_size,
    )

def validate_parquet(local_path):
    local_path = Path(local_path)

    try:
        parquet_file = pq.ParquetFile(local_path)

        if parquet_file.metadata.num_rows == 0:
            raise ValueError(
                f"Parquet file contains no rows: {local_path}"
            )

        logger.info(
            "Parquet validation passed: %s (%d rows)",
            local_path,
            parquet_file.metadata.num_rows,
        )

    except Exception as error:
        raise ValueError(
            f"Invalid Parquet file: {local_path}"
        ) from error

def build_s3_key(taxi_type, year_month, file_name, etl_batch_id):
    year, month = year_month.split("-")
    return (
        f"raw/taxi/{taxi_type}/"
        f"year={year}/"
        f"month={month}/"
        f"etl_batch_id={etl_batch_id}/"
        f"{file_name}"
    )

def upload_to_s3(s3_client, local_path, bucket, s3_key):
    logger.info("Uploading %s to s3://%s/%s", local_path, bucket, s3_key)
    s3_client.upload_file(
        str(local_path),
        bucket,
        s3_key
    )

    logger.info("Upload completed: s3://%s/%s", bucket, s3_key)

def s3_object_exists(s3_client, bucket, s3_key):
    try:
        s3_client.head_object(
            Bucket=bucket,
            Key=s3_key
        )
        logger.debug("S3 object exists: s3://%s/%s", bucket, s3_key)
        return True

    except ClientError as error:
        if error.response["Error"]["Code"] == "404":
            logger.debug("S3 object not found: s3://%s/%s", bucket, s3_key)
            return False
        raise

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = parse_arguments()
    taxi_type = args.taxi_type
    start_date = args.start_date
    end_date = args.end_date
    bucket_name = args.bucket_name
    source_system = args.source_system

    logger.info(
        "Starting TLC fetch: taxi_type=%s, start_date=%s, end_date=%s, bucket=%s, source_system=%s, phase=%s",
        taxi_type,
        start_date,
        end_date,
        bucket_name,
        source_system,
        args.phase_name,
    )


    etl_batch_id_file = Path(
    f"parfiles/{source_system}/etl_batch_id.txt"
)

    if not etl_batch_id_file.exists():
        raise FileNotFoundError(
        f"ETL batch ID file not found: {etl_batch_id_file}"
    )

    etl_batch_id = etl_batch_id_file.read_text().strip()

    if not etl_batch_id:
        raise ValueError(
        f"ETL batch ID file is empty: {etl_batch_id_file}"
    )

    logger.info("Using ETL batch ID %s from %s", etl_batch_id, etl_batch_id_file)

    s3_client = boto3.client("s3")

    months = generate_months(start_date, end_date)
    logger.info("Processing %d month(s): %s", len(months), ", ".join(months))

    for year_month in months:
        url = build_tlc_url(taxi_type, year_month)
        file_name = url.split("/")[-1] 
        local_path = Path(f"data/bronze/{file_name}")
        s3_key = build_s3_key(taxi_type, year_month, file_name, etl_batch_id)
        logger.info(
            "Processing %s data for %s: local_path=%s, s3_key=%s",
            taxi_type,
            year_month,
            local_path,
            s3_key,
        )
        if s3_object_exists(s3_client, bucket_name, s3_key):
            logger.info("Skipping upload; S3 object already exists: s3://%s/%s", bucket_name, s3_key)
        else:
            upload_to_s3(s3_client, local_path, bucket_name, s3_key)
        download_file(url, local_path)
        validate_download(local_path)
        validate_parquet(local_path)

    logger.info("TLC fetch completed successfully for %d month(s)", len(months))
if __name__ == "__main__":
    main()