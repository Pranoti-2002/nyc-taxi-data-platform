"""
------------------- Landing Archival Cleaner -------------------

Reads the Hive table LOCATION, finds all Hive partitions matching
the current etl_batch_id, and deletes the corresponding raw landing
files from S3.

The script is generic and does not assume any specific partition
columns such as year/month/day.

Usage:
    python landing_archival.py <source_system> <schema_name> <table_name>

Example:
    python landing_archival.py cv1 dataforge_landing fhv_trips

Example partition structures supported:

    year=2024/month=01/etl_batch_id=ABC

    year=2025/month=07/day=22/etl_batch_id=XYZ

    region=us/year=2026/month=09/etl_batch_id=PQR

    year=2026/etl_batch_id=ABC

-----------------------------------------------------------------
"""

import logging
import os
import re
import sys
from pathlib import Path

import boto3
from dotenv import load_dotenv

from generic_scripts.utils.s3_utils import (
    parse_s3_path,
    read_etl_batch_id,
    normalize_s3_uri,
)
from generic_scripts.utils.hive_connection import get_hive_connection


load_dotenv()


# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Get Hive table S3 location
# ------------------------------------------------------------------

def get_table_location(
    cursor,
    schema_name,
    table_name
):
    """
    Get the root S3 location of the Hive table.

    Example:

        s3a://dataforge-lake/raw/taxi/fhv/

    Returns:

        s3://dataforge-lake/raw/taxi/fhv
    """

    query = f"""
    SHOW CREATE TABLE {schema_name}.{table_name}
    """

    cursor.execute(query)

    rows = cursor.fetchall()

    create_table_lines = [
        row[0].strip()
        for row in rows
    ]

    try:

        location_line_index = next(
            index
            for index, line in enumerate(create_table_lines)
            if line.upper() == "LOCATION"
        )

        table_location = create_table_lines[
            location_line_index + 1
        ]

    except (StopIteration, IndexError) as error:

        raise ValueError(
            f"Could not find LOCATION in CREATE TABLE for "
            f"{schema_name}.{table_name}"
        ) from error

    # Remove quotes, commas and whitespace
    table_location = table_location.strip(
        "'\" ,"
    )

    # Normalize s3a://, s3n:// etc. to s3://
    table_location = normalize_s3_uri(
        table_location
    )

    if not table_location.startswith("s3://"):

        raise ValueError(
            f"Unsupported table location: "
            f"{table_location}"
        )

    table_location = table_location.rstrip("/")

    logger.info(
        "Table %s.%s root location: %s",
        schema_name,
        table_name,
        table_location
    )

    return table_location


# ------------------------------------------------------------------
# Get current ETL batch ID
# ------------------------------------------------------------------

def get_current_batch_id(
    source_system
):
    """
    Read the current ETL batch ID for the source system.

    Expected S3 path:

        s3://<bucket>/parfiles/<source_system>/
        <source_system>_batch_id.txt
    """

    bucket_name = os.getenv(
        "S3_BUCKET_NAME"
    )

    if not bucket_name:

        raise ValueError(
            "S3_BUCKET_NAME environment variable "
            "is not set"
        )

    etl_batch_id_path = (
        f"s3://{bucket_name}/"
        f"parfiles/{source_system}/"
        f"{source_system}_batch_id.txt"
    )

    logger.info(
        "Reading ETL batch ID from: %s",
        etl_batch_id_path
    )

    etl_batch_id = read_etl_batch_id(
        etl_batch_id_path=etl_batch_id_path,
        source_system=source_system
    )

    etl_batch_id = etl_batch_id.strip()

    if not etl_batch_id:

        raise ValueError(
            f"Empty etl_batch_id for source system "
            f"'{source_system}'"
        )

    logger.info(
        "Current etl_batch_id: %s",
        etl_batch_id
    )

    return etl_batch_id


# ------------------------------------------------------------------
# Find all matching Hive partitions
# ------------------------------------------------------------------

def get_batch_partitions(
    cursor,
    schema_name,
    table_name,
    etl_batch_id
):
    """
    Find every Hive partition containing the requested
    etl_batch_id.

    The function does not assume any partition structure.

    Examples of supported partitions:

        year=2024/month=01/etl_batch_id=ABC

        year=2024/month=01/day=22/etl_batch_id=ABC

        region=us/year=2024/month=01/etl_batch_id=ABC

    Returns:
        List of complete partition paths.
    """

    query = f"""
    SHOW PARTITIONS {schema_name}.{table_name}
    """

    logger.info(
        "Fetching partitions for %s.%s",
        schema_name,
        table_name
    )

    cursor.execute(query)

    rows = cursor.fetchall()

    if not rows:

        raise ValueError(
            f"No partitions found for "
            f"{schema_name}.{table_name}"
        )

    partition_key = (
        f"etl_batch_id={etl_batch_id}"
    )

    matching_partitions = []

    for row in rows:

        partition = row[0].strip()

        partition_parts = partition.split("/")

        # Match etl_batch_id as a complete partition
        # component rather than using substring matching.
        if partition_key in partition_parts:

            matching_partitions.append(
                partition
            )

    if not matching_partitions:

        raise ValueError(
            f"No partitions found for "
            f"etl_batch_id={etl_batch_id} "
            f"in {schema_name}.{table_name}"
        )

    logger.info(
        "Found %d matching partition(s) "
        "for etl_batch_id=%s",
        len(matching_partitions),
        etl_batch_id
    )

    for partition in matching_partitions:

        logger.info(
            "Matching partition: %s",
            partition
        )

    return matching_partitions


# ------------------------------------------------------------------
# Resolve S3 paths for current batch
# ------------------------------------------------------------------

def extract_partition_columns(create_table_lines):
    """Extract partition column names from a Hive SHOW CREATE TABLE output."""
    ddl = "\n".join(create_table_lines)
    match = re.search(
        r"PARTITIONED\s+BY\s*\((.*?)\)\s*(?:ROW\s+FORMAT|STORED\s+AS|LOCATION|TBLPROPERTIES|$)",
        ddl,
        flags=re.IGNORECASE | re.DOTALL,
    )

    if not match:
        return []

    partition_block = match.group(1)
    return re.findall(
        r"\b([A-Za-z0-9_]+)\b\s+(?:STRING|INT|BIGINT|DOUBLE|FLOAT|DECIMAL|TIMESTAMP|BOOLEAN|CHAR|VARCHAR|DATE|ARRAY<.*>|MAP<.*>|STRUCT<.*>)",
        partition_block,
        flags=re.IGNORECASE,
    )


def get_table_s3(cursor, schema_name, table_name, source_system):
    """Build the current raw S3 batch path for a Hive table, including partition pattern when present."""
    query = f"""
    SHOW CREATE TABLE {schema_name}.{table_name}
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    create_table_lines = [row[0].strip() for row in rows]

    ddl = "\n".join(create_table_lines)
    location_match = re.search(r"LOCATION\s+['\"]?(s3a?://[^'\"\s]+)", ddl, flags=re.IGNORECASE)

    if location_match:
        table_location = location_match.group(1)
    else:
        try:
            location_line_index = next(
                index for index, line in enumerate(create_table_lines)
                if line.upper() == "LOCATION"
            )
            table_location = create_table_lines[location_line_index + 1]
        except (StopIteration, IndexError) as error:
            raise ValueError(
                f"Could not find LOCATION in CREATE TABLE for {schema_name}.{table_name}"
            ) from error

    table_location = normalize_s3_uri(table_location.strip("'\" ,"))
    if not table_location.startswith("s3://"):
        raise ValueError(f"Unsupported table location: {table_location}")

    table_path = Path(table_location.replace("s3://", "", 1))
    table_parts = table_path.parts
    if table_parts and table_parts[-1] == table_name:
        table_path = Path(*table_parts[:-1])

    cleaned_table_location = f"s3://{table_path.as_posix()}" if table_path.parts else "s3://"

    bucket_name = os.getenv("S3_BUCKET_NAME")
    etl_batch_id_path = f"s3://{bucket_name}/parfiles/{source_system}/{source_system}_batch_id.txt"
    etl_batch_id = read_etl_batch_id(etl_batch_id_path=etl_batch_id_path, source_system=source_system)

    partition_columns = extract_partition_columns(create_table_lines)
    if partition_columns:
        partition_suffix = []
        for column in partition_columns:
            if column.lower() == "etl_batch_id":
                partition_suffix.append(f"etl_batch_id={etl_batch_id}")
            else:
                partition_suffix.append(f"{column}=*")
        return f"{cleaned_table_location.rstrip('/')}/" + "/".join(partition_suffix)

    return cleaned_table_location + "/" + etl_batch_id


def refresh_hive_metadata(cursor, schema_name, table_name):
    """Refresh the Hive metastore after deleting partition directories from S3."""
    repair_sql = f"MSCK REPAIR TABLE {schema_name}.{table_name}"
    logger.info("Refreshing Hive metadata with: %s", repair_sql)
    cursor.execute(repair_sql)


def get_batch_s3_paths(
    cursor,
    schema_name,
    table_name,
    source_system
):
    """
    Resolve all S3 paths belonging to the current ETL batch.

    Returns a list because the same etl_batch_id may exist
    under multiple partition combinations.

    Example:

        [
            "s3://dataforge-lake/raw/taxi/fhv/"
            "year=2024/month=01/"
            "etl_batch_id=ABC/",

            "s3://dataforge-lake/raw/taxi/fhv/"
            "year=2024/month=02/"
            "etl_batch_id=ABC/"
        ]
    """

    # --------------------------------------------------------------
    # Get table root S3 location
    # --------------------------------------------------------------

    table_location = get_table_location(
        cursor,
        schema_name,
        table_name
    )

    # --------------------------------------------------------------
    # Get current batch ID
    # --------------------------------------------------------------

    etl_batch_id = get_current_batch_id(
        source_system
    )

    # --------------------------------------------------------------
    # Find every matching partition
    # --------------------------------------------------------------

    partitions = get_batch_partitions(
        cursor,
        schema_name,
        table_name,
        etl_batch_id
    )

    # --------------------------------------------------------------
    # Build complete S3 paths
    # --------------------------------------------------------------

    batch_s3_paths = []

    for partition in partitions:

        raw_batch_path = (
            f"{table_location.rstrip('/')}/"
            f"{partition.rstrip('/')}/"
        )

        batch_s3_paths.append(
            raw_batch_path
        )

        logger.info(
            "Resolved batch S3 path: %s",
            raw_batch_path
        )

    return batch_s3_paths


# ------------------------------------------------------------------
# Delete S3 prefix
# ------------------------------------------------------------------

def delete_s3_prefix(
    s3_path
):
    """
    Delete every S3 object under the supplied prefix.

    S3 does not have real directories. Therefore the function
    lists every object under the prefix and deletes the objects.
    """

    s3 = boto3.client(
        "s3"
    )

    bucket, prefix = parse_s3_path(
        s3_path
    )

    prefix = prefix.rstrip("/") + "/"

    logger.info(
        "Deleting S3 objects under: "
        "s3://%s/%s",
        bucket,
        prefix
    )

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    deleted_count = 0

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix
    ):

        objects = page.get(
            "Contents",
            []
        )

        if not objects:
            continue

        objects_to_delete = [
            {
                "Key": obj["Key"]
            }
            for obj in objects
        ]

        # list_objects_v2 returns at most 1000 objects
        # per page, which is also the maximum supported
        # by delete_objects.
        response = s3.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": objects_to_delete,
                "Quiet": True
            }
        )

        errors = response.get(
            "Errors",
            []
        )

        if errors:

            for error in errors:

                logger.error(
                    "Failed to delete S3 object %s: %s",
                    error.get("Key"),
                    error.get("Message")
                )

            raise RuntimeError(
                f"Failed to delete {len(errors)} "
                f"S3 object(s) under "
                f"s3://{bucket}/{prefix}"
            )

        deleted_count += len(
            objects_to_delete
        )

        logger.info(
            "Deleted %d objects from current page",
            len(objects_to_delete)
        )

    if deleted_count == 0:

        logger.info(
            "No objects found under "
            "s3://%s/%s",
            bucket,
            prefix
        )

    else:

        logger.info(
            "Successfully deleted %d objects from "
            "s3://%s/%s",
            deleted_count,
            bucket,
            prefix
        )
        


# ------------------------------------------------------------------
# Clear local landing files
# ------------------------------------------------------------------

def clear_local_landing_files(
    keep_latest: int = 3,
    base_dir: str = "/opt/project/data/bronze"
):
    """
    Delete older local landing files and retain
    the latest `keep_latest` files.
    """

    local_dir = Path(
        base_dir
    )

    if not local_dir.exists():

        logger.info(
            "Local landing directory does not exist: %s",
            local_dir
        )

        return

    files = [
        file_path
        for file_path in local_dir.iterdir()
        if file_path.is_file()
    ]

    if len(files) <= keep_latest:

        logger.info(
            "Local landing files count (%d) is within "
            "retention limit (%d); no cleanup needed.",
            len(files),
            keep_latest
        )

        return

    # Newest files first
    files_sorted = sorted(
        files,
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )

    files_to_delete = files_sorted[
        keep_latest:
    ]

    for file_path in files_to_delete:

        try:

            file_path.unlink()

            logger.info(
                "Deleted old local landing file: %s",
                file_path
            )

        except Exception as error:

            logger.warning(
                "Failed to delete local landing file %s: %s",
                file_path,
                error
            )


# ------------------------------------------------------------------
# Clear raw files
# ------------------------------------------------------------------

def clear_raw_files(
    cursor,
    schema_name,
    table_name,
    source_system
):
    """
    Resolve all S3 partitions belonging to the current
    ETL batch and delete their objects.
    """

    batch_s3_paths = get_batch_s3_paths(
        cursor,
        schema_name,
        table_name,
        source_system
    )

    # --------------------------------------------------------------
    # Delete every matching partition
    # --------------------------------------------------------------

    for raw_batch_path in batch_s3_paths:

        logger.info(
            "Clearing raw data from: %s",
            raw_batch_path
        )

        try:

            delete_s3_prefix(
                raw_batch_path
            )

        except Exception as error:

            logger.error(
                "Failed to clear S3 path %s: %s",
                raw_batch_path,
                error
            )

            # Continue with other matching partitions
            continue

    try:
        refresh_hive_metadata(cursor, schema_name, table_name)
    except Exception as error:
        logger.warning(
            "Hive metadata refresh failed for %s.%s after S3 cleanup: %s",
            schema_name,
            table_name,
            error,
        )

    # --------------------------------------------------------------
    # Local landing cleanup
    # --------------------------------------------------------------

    clear_local_landing_files(
        keep_latest=3
    )


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    """
    Main entry point for landing archival cleanup.
    """

    # --------------------------------------------------------------
    # Validate command-line arguments
    # --------------------------------------------------------------

    if len(sys.argv) != 4:

        print(
            "Usage: python landing_archival.py "
            "<source_system> <schema_name> <table_name>"
        )

        sys.exit(1)

    source_system = (
        sys.argv[1].strip("'\"")
    )

    schema_name = (
        sys.argv[2].strip("'\"")
    )

    table_name = (
        sys.argv[3].strip("'\"")
    )

    conn = None
    cursor = None

    try:

        # ----------------------------------------------------------
        # Connect to Hive
        # ----------------------------------------------------------

        conn = get_hive_connection()

        if not conn:

            logger.error(
                "Can't connect to Hive. "
                "Please check DB availability."
            )

            sys.exit(1)

        cursor = conn.cursor()

        logger.info(
            "Connected successfully to Hive"
        )

        # ----------------------------------------------------------
        # Clear current batch
        # ----------------------------------------------------------

        clear_raw_files(
            cursor,
            schema_name,
            table_name,
            source_system
        )

    except Exception as error:

        logger.error(
            "Failed to clean files: %s",
            error,
            exc_info=True
        )

        sys.exit(1)

    finally:

        if cursor:

            try:
                cursor.close()
            except Exception:
                pass

        if conn:

            try:
                conn.close()
            except Exception:
                pass


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

if __name__ == "__main__":
    main()
