"""------------------- Taxi Zone Lookup Fetcher -------------------
Downloads the NYC taxi zone lookup CSV and shapefile archive, converts the zones to GeoParquet, and uploads the lookup data to S3.
Usage: python lookup_fetcher.py
Example: python lookup_fetcher.py
This script stores downloaded lookup files under data/bronze/lookup and uploads the CSV and GeoParquet outputs to the configured lookup keys.
------------------- ------------------- ----------------------"""

import logging
import shutil
import zipfile
from pathlib import Path
import os
import boto3
import geopandas as gpd
import requests
from dotenv import load_dotenv
from generic_scripts.utils.s3_utils import write_s3_path,s3_object_exists

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


TAXI_ZONE_LOOKUP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/misc/"
    "taxi_zone_lookup.csv"
)

TAXI_ZONES_ZIP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/misc/"
    "taxi_zones.zip"
)


LOCAL_LOOKUP_DIR = Path("/opt/project/data/bronze/lookup")

LOOKUP_CSV_PATH = (
    LOCAL_LOOKUP_DIR / "taxi_zone_lookup.csv"
)

TAXI_ZONES_ZIP_PATH = (
    LOCAL_LOOKUP_DIR / "taxi_zones.zip"
)

TAXI_ZONES_EXTRACT_DIR = (
    LOCAL_LOOKUP_DIR / "taxi_zones"
)

TAXI_ZONES_PARQUET_PATH = (
    LOCAL_LOOKUP_DIR / "taxi_zones.parquet"
)


LOOKUP_CSV_S3_KEY = (
    "/raw/lookup/taxi_zone_loc_ref/taxi_zone_lookup.csv"
)

TAXI_ZONES_PARQUET_S3_KEY = (
    "raw/lookup/taxi_zone_shapefile/taxi_zones.parquet"
)


def download_file(
    url: str,
    output_path: Path,
) -> Path:
    """
    Download a file from a URL.
    """

    output_path.parent.mkdir(parents=True,exist_ok=True,)

    logger.info(
        "Downloading %s",
        url,
    )

    response = requests.get(
        url,
        timeout=60,
    )

    response.raise_for_status()

    output_path.write_bytes(
        response.content
    )

    logger.info(
        "Downloaded successfully: %s",
        output_path,
    )

    return output_path


def extract_taxi_zones(
    zip_path: Path,
    extract_dir: Path,
) -> Path:
    """
    Extract the taxi zones ZIP file.
    """

    if extract_dir.exists():
        shutil.rmtree(extract_dir)

    extract_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    logger.info(
        "Extracting %s",
        zip_path,
    )

    with zipfile.ZipFile(
        zip_path,
        "r",
    ) as zip_file:
        zip_file.extractall(
            extract_dir
        )

    logger.info(
        "Taxi zones extracted to: %s",
        extract_dir,
    )

    return extract_dir


def convert_taxi_zones_to_parquet(
    extract_dir: Path,
    output_path: Path,
) -> Path:
    """
    Read the extracted shapefile and convert it
    to GeoParquet.
    """

    shapefile_path = (
        extract_dir
        / "taxi_zones"
        / "taxi_zones.shp"
    )

    if not shapefile_path.exists():
        raise FileNotFoundError(
            f"Taxi zone shapefile not found: "
            f"{shapefile_path}"
        )

    logger.info(
        "Reading taxi zone shapefile: %s",
        shapefile_path,
    )

    taxi_zones = gpd.read_file(
        shapefile_path
    )

    logger.info(
        "Loaded %s taxi zones",
        len(taxi_zones),
    )

    logger.info(
        "Source CRS: %s",
        taxi_zones.crs,
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    taxi_zones.to_parquet(
        output_path,
        index=False,
    )

    logger.info(
        "GeoParquet created: %s",
        output_path,
    )

    return output_path



def main() -> None:
    
    load_dotenv()
    bucket_name = os.getenv("S3_BUCKET_NAME")

    # Check if the file already exists
    if s3_object_exists(bucket_name, LOOKUP_CSV_S3_KEY) :
        logger.info("Skipping upload; S3 object already exists: s3://%s/%s", bucket_name, LOOKUP_CSV_S3_KEY)
    if s3_object_exists(bucket_name, TAXI_ZONES_PARQUET_S3_KEY):
        logger.info("Skipping upload; S3 object already exists: s3://%s/%s", bucket_name, TAXI_ZONES_PARQUET_S3_KEY)    
    else:                       
        # Download Taxi Zone Lookup CSV                            
        download_file(
            url=TAXI_ZONE_LOOKUP_URL,
            output_path=LOOKUP_CSV_PATH,
        )
        # Upload Taxi Zone Lookup CSV

        write_s3_path(
            file_path=LOOKUP_CSV_PATH,
            bucket=bucket_name,
            s3_key=LOOKUP_CSV_S3_KEY,
        )

        # Download Taxi Zones ZIP

        download_file(
            url=TAXI_ZONES_ZIP_URL,
            output_path=TAXI_ZONES_ZIP_PATH,
        )

        # Extract ZIP

        extract_taxi_zones(
            zip_path=TAXI_ZONES_ZIP_PATH,
            extract_dir=TAXI_ZONES_EXTRACT_DIR,
        )
        # Convert Shapefile → GeoParquet

        convert_taxi_zones_to_parquet(
            extract_dir=TAXI_ZONES_EXTRACT_DIR,
            output_path=TAXI_ZONES_PARQUET_PATH,
        )

        # Upload GeoParquet to S3
        write_s3_path(
            file_path=TAXI_ZONES_PARQUET_PATH,
            bucket=bucket_name,
            s3_key=TAXI_ZONES_PARQUET_S3_KEY,
        )


if __name__ == "__main__":
    main()