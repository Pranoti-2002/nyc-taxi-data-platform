"""------------------- Landing Archival Cleaner -------------------
Reads the table LOCATION for a Hive table, resolves the current etl_batch_id for the source system, and deletes the matching raw landing files from S3 so older batches are archived or cleaned before the next run.
Usage: python landing_archival.py <source_system> <schema_name> <table_name>
example: python landing_archival.py cv1 dataforge_audit batch_log
This script normalizes s3a/s3 URIs, resolves the target batch path, and removes files under the matching raw-data prefix from S3.
------------------- ------------------- ----------------------"""

from datetime import datetime
import logging
import sys
from generic_scripts.utils.s3_utils import delete_s3_path_data, parse_s3_path, read_etl_batch_id, normalize_s3_uri
from generic_scripts.utils.hive_connection import get_hive_connection
from pathlib import Path
from dotenv import load_dotenv
import os


load_dotenv()

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_table_s3(cursor, schema_name, table_name, source_system):
    # Read S3 location from table through hive
    query = f"""
    show create table {schema_name}.{table_name}
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    create_table_lines = [row[0].strip() for row in rows]

    try:
        location_line_index = next(
            index for index, line in enumerate(create_table_lines)
            if line.upper() == "LOCATION"
        )
        table_location = create_table_lines[location_line_index + 1]
    except (StopIteration, IndexError) as error:
        raise ValueError(
            f"Could not find LOCATION in CREATE TABLE for "
            f"{schema_name}.{table_name}"
        ) from error

    table_location = normalize_s3_uri(table_location.strip("'\" ,"))
    if not table_location.startswith("s3://"):
        raise ValueError(f"Unsupported table location: {table_location}")
    logger.info("Table %s.%s is stored at %s", schema_name, table_name, table_location)

    table_path = Path(table_location.replace("s3://", "", 1))
    table_parts = table_path.parts
    if table_parts and table_parts[-1] == table_name:
        table_path = Path(*table_parts[:-1])

    cleaned_table_location = f"s3://{table_path.as_posix()}" if table_path.parts else "s3://"

    # Read etl_batch_id for the source_system
    bucket_name = str(os.getenv("S3_BUCKET_NAME"))
    etl_batch_id_path = "s3://" + bucket_name + "/parfiles/" + f"{source_system}/" + f"{source_system}_batch_id.txt"
    etl_batch_id = read_etl_batch_id(etl_batch_id_path=etl_batch_id_path, source_system=source_system)
    return cleaned_table_location + "/" + etl_batch_id

def clear_local_landing_files(keep_latest: int = 3, base_dir: str = "/opt/project/data/bronze"):
    """Delete older local landing files when there are more than keep_latest files."""
    local_dir = Path(base_dir)
    if not local_dir.exists():
        logger.info("Local landing directory does not exist: %s", local_dir)
        return

    files = [
        file_path for file_path in local_dir.iterdir()
        if file_path.is_file()
    ]
    if len(files) <= keep_latest:
        logger.info(
            "Local landing files count (%d) is within retention limit (%d); no cleanup needed.",
            len(files),
            keep_latest,
        )
        return

    files_sorted = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)
    files_to_delete = files_sorted[keep_latest:]

    for file_path in files_to_delete:
        try:
            file_path.unlink()
            logger.info("Deleted old local landing file: %s", file_path)
        except Exception as exc:
            logger.warning("Failed to delete local landing file %s: %s", file_path, exc)


def clear_raw_files(cursor, schema_name, table_name, source_system):
    """Remove older raw files on S3 and keep only the newest local landing files."""
    raw_dir = get_table_s3(cursor, schema_name, table_name, source_system)
    logger.info(raw_dir)
    bucket, key = parse_s3_path(raw_dir)
    logger.info(bucket)
    logger.info(key)
    try:
        logger.info(f"Clearing raw data from s3 path: {raw_dir}")
        delete_s3_path_data(bucket, key)
        logger.info("Successfully cleared s3 path")
    except Exception as E:
        logger.warning(f"Failed to clear data from the s3 path: {E}")

    clear_local_landing_files(keep_latest=3)


def main():
    """Main entry point for clearing raw_data."""
    # Validate arguments
    if len(sys.argv) !=4:
        print("Usage: python landing_archival.py <source_system>")
        sys.exit(1)
    source_system = sys.argv[1].strip("'")
    schema_name = sys.argv[2].strip("'")
    table_name = sys.argv[3].strip("'")
    cursor = None  
    
    try:
        # Connect to database
        conn = get_hive_connection()
        if not conn:
            logger.error("Can't connect to Hive. Please check DB availability.")
            sys.exit(1)
        cursor = conn.cursor()
        logger.info("Connected successfully to Hive")
        clear_raw_files(cursor, schema_name, table_name, source_system)
    except Exception as E:
        logger.error(f"Failed to clean files: {E}")

        
if __name__ == "__main__":
    main()
    