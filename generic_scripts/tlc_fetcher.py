"""------------------- TLC Trip Data Fetcher -------------------
Downloads monthly NYC Taxi and Limousine Commission trip data, validates each Parquet file, and uploads it to the raw S3 partition for the current ETL batch.
Usage: python tlc_fetcher.py --taxi-type TYPE --start-date YYYY-MM --end-date YYYY-MM <source_system> <phase_name>
Example: python tlc_fetcher.py --taxi-type yellow --start-date 2024-01 --end-date 2024-02 nyc_taxi ingestion
This script skips an upload when the corresponding batch-partitioned object already exists in S3.
------------------- ------------------- ----------------------"""

import argparse
from datetime import datetime
from pathlib import Path
import logging

import boto3
import requests

try:
    import pyarrow.parquet as pq
except ModuleNotFoundError:  # pragma: no cover - optional dependency for local/test import
    pq = None

from generic_scripts.utils.hive_connection import get_hive_connection
from generic_scripts.utils.s3_utils import (
    get_bucket_name,
    read_etl_batch_id,
    s3_object_exists,
    write_s3_path,
)


logger = logging.getLogger(__name__)
s3_client = boto3.client("s3")

TAXI_FILE_PREFIX = {
    "yellow": "yellow_tripdata",
    "green": "green_tripdata",
    "fhv": "fhv_tripdata",
    "hvfhv": "fhvhv_tripdata",
}

TAXI_TABLE_MAP = {
    "yellow": "yellow_taxi",
    "green": "green_taxi",
    "fhv": "fhv_trips",
    "hvfhv": "fhvhv_trips",
}

# Note: Add checksum for source data check

def parse_arguments():
    parser = argparse.ArgumentParser(description="Download NYC TLC data")

    parser.add_argument(
        "--taxi-type",
        nargs="+",
        required=True,
        choices=["yellow", "green", "fhv", "hvfhv"],
        help="One or more taxi types to fetch (yellow, green, fhv, hvfhv)",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM format",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM format",
    )
    parser.add_argument(
        "source_system",
        help="Source system name (for example: geo_sup_bs_6)",
    )
    parser.add_argument(
        "phase_name",
        nargs="?",
        default="landing",
        help="Pipeline phase name. Defaults to landing.",
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

def refresh_hive_metadata(cursor, schema_name, table_name):
    """Refresh Hive metadata after new raw partitions are uploaded to S3."""
    repair_sql = f"MSCK REPAIR TABLE {schema_name}.{table_name}"
    logger.info("Refreshing Hive metadata with: %s", repair_sql)
    cursor.execute(repair_sql)


def validate_parquet(local_path):
    local_path = Path(local_path)

    if pq is None:
        raise ModuleNotFoundError("pyarrow is required to validate parquet files")

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


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = parse_arguments()
    taxi_types = args.taxi_type
    start_date = args.start_date
    end_date = args.end_date
    bucket_name = get_bucket_name()
    source_system = args.source_system
    phase_name = args.phase_name

    etl_batch_id_path = f"s3://{bucket_name}/parfiles/{source_system}/{source_system}_batch_id.txt"
    logger.info("Using source_system=%s, phase_name=%s, batch_id_path=%s", source_system, phase_name, etl_batch_id_path)
    etl_batch_id = read_etl_batch_id(etl_batch_id_path, source_system)

    if not etl_batch_id:
        raise ValueError(f"ETL batch ID file is empty: {etl_batch_id_path}")

    logger.info("Using ETL batch ID %s from %s", etl_batch_id, etl_batch_id_path)

    months = generate_months(start_date, end_date)
    logger.info("Processing %d month(s): %s", len(months), ", ".join(months))

    for taxi_type in taxi_types:
        for year_month in months:
            url = build_tlc_url(taxi_type, year_month)
            file_name = url.split("/")[-1]
            local_path = Path("/opt/project/data/bronze") / file_name
            s3_key = build_s3_key(taxi_type, year_month, file_name, etl_batch_id)
            logger.info(
                "Processing %s data for %s: local_path=%s, s3_key=%s",
                taxi_type,
                year_month,
                local_path,
                s3_key,
            )
            if s3_object_exists(bucket_name, s3_key):
                logger.info("Skipping upload; S3 object already exists: s3://%s/%s", bucket_name, s3_key)
            else:
                download_file(url, local_path)
                validate_download(local_path)
                validate_parquet(local_path)
                write_s3_path(bucket_name, s3_key, local_path)

                try:
                    table_name = TAXI_TABLE_MAP.get(taxi_type)
                    if not table_name:
                        raise ValueError(f"No Hive table mapping found for taxi type '{taxi_type}'")
                    conn = get_hive_connection()
                    cursor = conn.cursor()
                    refresh_hive_metadata(cursor, "dataforge_landing", table_name)
                    cursor.close()
                    conn.close()
                    logger.info("Refreshed Hive metadata for dataforge_landing.%s", table_name)
                except Exception as error:
                    logger.warning(
                        "Hive metadata refresh failed after uploading %s/%s: %s",
                        taxi_type,
                        year_month,
                        error,
                    )

    logger.info("TLC fetch completed successfully for %d month(s)", len(months))


if __name__ == "__main__":
    main()