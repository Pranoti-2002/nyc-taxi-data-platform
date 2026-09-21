"""------------------- TLC Trip Data Fetcher -------------------
Downloads monthly NYC Taxi and Limousine Commission trip data, validates each Parquet file, and uploads it to the raw S3 partition for the current ETL batch.
Usage: python tlc_fetcher.py --taxi-type TYPE --start-date YYYY-MM --end-date YYYY-MM <source_system> <phase_name>
Example: python tlc_fetcher.py --taxi-type yellow --start-date 2024-01 --end-date 2024-02 nyc_taxi ingestion
This script skips an upload when the corresponding batch-partitioned object already exists in S3.
------------------- ------------------- ----------------------"""

import argparse
import json
from datetime import datetime
from pathlib import Path
import logging

import boto3
import pyarrow.parquet as pq
import requests

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

TAXI_TYPE_ALIASES = {
    "yellow": {"yellow", "yellow_taxi", "yellow_taxi_ingest"},
    "green": {"green", "green_taxi", "green_taxi_ingest"},
    "fhv": {"fhv", "fhv_trips", "fhv_trips_ingest"},
    "hvfhv": {"hvfhv", "fhvhv_trips", "fhvhv_trips_ingest"},
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
        nargs="?",
        default=None,
        help="Optional source system name. If omitted, it is inferred from S3 parameter files.",
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


def list_parameter_files(bucket_name, source_system):
    prefix = f"parfiles/{source_system}/"
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return [
        item["Key"]
        for item in response.get("Contents", [])
        if item["Key"].endswith("_prm.json")
    ]


def read_parameter_file(bucket_name, key):
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def taxi_type_matches_parameter(taxi_type, parameter_row):
    match_tokens = TAXI_TYPE_ALIASES.get(taxi_type, {taxi_type})
    wf_name = str(parameter_row.get("wf_name", "")).lower()
    source_table = str(parameter_row.get("source_table", "")).lower()
    target_table = str(parameter_row.get("target_table", "")).lower()
    return any(
        token in wf_name or token in source_table or token in target_table
        for token in match_tokens
    )


def resolve_source_system(bucket_name, taxi_types):
    paginator = s3_client.get_paginator("list_objects_v2")
    candidates = set()

    for page in paginator.paginate(Bucket=bucket_name, Prefix="parfiles/"):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith("_prm.json"):
                continue

            parts = key.split("/")
            if len(parts) < 3:
                continue

            source_system = parts[1]
            parameter_row = read_parameter_file(bucket_name, key)
            if any(taxi_type_matches_parameter(taxi_type, parameter_row) for taxi_type in taxi_types):
                candidates.add(source_system)

    if len(candidates) == 1:
        return next(iter(candidates))
    if len(candidates) > 1:
        raise ValueError(
            f"Multiple source systems match the requested taxi type(s): {sorted(candidates)}. "
            "Please pass source_system explicitly."
        )
    return None


def resolve_workflow_parameters(bucket_name, source_system, taxi_types):
    workflow_parameters = {}
    for key in list_parameter_files(bucket_name, source_system):
        parameter_row = read_parameter_file(bucket_name, key)
        for taxi_type in taxi_types:
            if taxi_type_matches_parameter(taxi_type, parameter_row):
                workflow_parameters[taxi_type] = parameter_row
                logger.info(
                    "Loaded workflow parameters for %s from s3://%s/%s",
                    taxi_type,
                    bucket_name,
                    key,
                )
    return workflow_parameters


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

    if source_system is None:
        source_system = resolve_source_system(bucket_name, taxi_types)
        if source_system is None:
            raise ValueError(
                "Could not resolve a source_system from S3 parameter files. "
                "Please pass source_system explicitly."
            )
        logger.warning(
            "No source system was supplied; inferred source_system='%s' from S3 parameter files.",
            source_system,
        )

    etl_batch_id_path = f"s3://{bucket_name}/parfiles/{source_system}/{source_system}_batch_id.txt"
    logger.info(etl_batch_id_path)
    workflow_parameters = resolve_workflow_parameters(bucket_name, source_system, taxi_types)
    etl_batch_id = read_etl_batch_id(etl_batch_id_path, source_system)

    if not etl_batch_id:
        raise ValueError(f"ETL batch ID file is empty: {etl_batch_id_path}")

    logger.info("Using ETL batch ID %s from %s", etl_batch_id, etl_batch_id_path)

    months = generate_months(start_date, end_date)
    logger.info("Processing %d month(s): %s", len(months), ", ".join(months))

    for taxi_type in taxi_types:
        parameter_row = workflow_parameters.get(taxi_type)
        target_table = taxi_type
        source_schema = "NYC_GOV"
        target_schema = "dataforge_landing"

        if parameter_row:
            source_schema = parameter_row.get("source_schema") or source_schema
            target_schema = parameter_row.get("target_schema") or target_schema
            target_table = parameter_row.get("target_table") or parameter_row.get("source_table") or target_table
            etl_batch_id = parameter_row.get("etl_batch_id") or etl_batch_id
            logger.info(
                "Using workflow metadata for %s: source_schema=%s, target_schema=%s, target_table=%s, etl_batch_id=%s",
                taxi_type,
                source_schema,
                target_schema,
                target_table,
                etl_batch_id,
            )

        for year_month in months:
            url = build_tlc_url(taxi_type, year_month)
            file_name = url.split("/")[-1]
            local_path = Path("/opt/project/data/bronze") / file_name
            s3_key = build_s3_key(taxi_type, year_month, file_name, etl_batch_id)
            logger.info(
                "Processing %s data for %s: local_path=%s, s3_key=%s, target_table=%s",
                taxi_type,
                year_month,
                local_path,
                s3_key,
                target_table,
            )
            if s3_object_exists(bucket_name, s3_key):
                logger.info("Skipping upload; S3 object already exists: s3://%s/%s", bucket_name, s3_key)
            else:
                download_file(url, local_path)
                validate_download(local_path)
                validate_parquet(local_path)
                write_s3_path(bucket_name, s3_key, local_path)

    logger.info("TLC fetch completed successfully for %d month(s)", len(months))


if __name__ == "__main__":
    main()