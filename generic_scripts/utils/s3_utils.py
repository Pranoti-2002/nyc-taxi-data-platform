"""------------------- S3 Utility Helpers -------------------
Provides shared S3 helpers for normalizing S3 URIs, parsing bucket/key pairs, reading and writing objects, deleting object prefixes, checking file existence, and falling back to the local parfiles directory when a batch id file is unavailable in S3.
Usage: imported by ETL scripts; e.g. read_etl_batch_id(...), delete_s3_path_data(bucket, key)
example: s3://dataforge-lake/parfiles/cv1/etl_batch_id.txt
This module centralizes all S3 access patterns used by the ingestion and archival workflows.
------------------- ------------------- ----------------------"""

from urllib import response
from urllib.parse import urlparse
from pathlib import Path
import boto3
import logging
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv
s3 = boto3.client('s3')

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def normalize_s3_uri(s3_path):
    if not s3_path:
        return s3_path
    normalized = s3_path.strip().strip("'\"")
    if normalized.startswith("s3a://"):
        normalized = "s3://" + normalized[len("s3a://"):]
    return normalized.rstrip("/")


def parse_s3_path(s3_path):
    normalized_path = normalize_s3_uri(s3_path)
    parsed = urlparse(normalized_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key


def read_s3_path_data(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    # Read the content
    content_bytes = response['Body'].read()
    # Decode bytes to string (e.g., UTF-8)
    content_str = content_bytes.decode('utf-8').strip()
    return content_str
    
    
def write_s3_path(bucket, s3_key, file_path):
    logger.info("Uploading %s to s3://%s/%s", file_path, bucket, s3_key)
    s3.upload_file(
        str(file_path),
        bucket,
        s3_key
    )
    logger.info("Upload completed: s3://%s/%s", bucket, s3_key)

    
def delete_s3_path_data(bucket, key):
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=key).get("Contents", [])
    if not objects:
        logger.info("No objects found under S3 prefix %s/%s to delete", bucket, key)
        return

    delete_payload = {
        "Objects": [{"Key": obj["Key"]} for obj in objects],
        "Quiet": True,
    }
    s3.delete_objects(Bucket=bucket, Delete=delete_payload)


def s3_object_exists(bucket, s3_key):
    try:
        s3.head_object(Bucket=bucket,Key=s3_key)
        logger.debug("S3 object exists: s3://%s/%s", bucket, s3_key)
        return True
    except ClientError as error:
        if error.response["Error"]["Code"] == "404":
            logger.debug("S3 object not found: s3://%s/%s", bucket, s3_key)
            return False
        raise
            
def read_etl_batch_id(etl_batch_id_path, source_system):
    try:
        # Read etl_batch_id from s3 using boto3
        bucket_name, file_key = parse_s3_path(etl_batch_id_path)
        logger.info("Successfully parsed s3 path for etl_batch_id")
        etl_batch_id = read_s3_path_data(bucket_name, file_key)
        logger.info(f"Sucesfully read etl_batch_id from bucket {bucket_name} and key {file_key}")
        return etl_batch_id
    except Exception as E:
        logger.warning(f"failed to get etl_batch_id from s3: {E}")
        logger.info("Reverting back to local file path")

    project_root = Path(__file__).resolve().parents[2]
    etl_batch_id_path = project_root / "parfiles" / source_system / f"{source_system}_batch_id.txt"

    with open(etl_batch_id_path, "r", encoding="utf-8") as f:
        etl_batch_id = f.readline().strip()
    return etl_batch_id

def get_bucket_name():
    load_dotenv(
        dotenv_path=Path(__file__).resolve().parent.parent / ".env",
        override=False,
    )
    bucket_name = os.getenv("S3_BUCKET_NAME")

    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME is not set in the environment or .env file")

    return bucket_name