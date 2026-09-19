"""------------------- NYC Weather Fetcher -------------------
Fetches historical hourly NYC weather data from Open-Meteo, validates the response, saves monthly JSON files, and uploads them to the raw S3 partition for the current ETL batch.
Usage: python weather_fetcher.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD --bucket-name BUCKET <source_system>
Example: python weather_fetcher.py --start-date 2024-01-01 --end-date 2024-02-29 --bucket-name dataforge-lake nyc_weather
This script processes the requested date range month by month and stores local output under data/bronze/weather.
------------------- ------------------- ----------------------"""

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
import boto3 
import calendar
import requests


OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

NYC_LATITUDE = 40.7128
NYC_LONGITUDE = -74.0060
NYC_TIMEZONE = "America/New_York"

HOURLY_VARIABLES = [
    "temperature_2m",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "wind_speed_10m",
]

DEFAULT_OUTPUT_DIR = Path("data/bronze/weather")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def validate_date(date_string: str) -> datetime:
    """
    Validate YYYY-MM-DD date format.
    """
    try:
        return datetime.strptime(date_string, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"Invalid date '{date_string}'. Expected format: YYYY-MM-DD"
        ) from exc


def fetch_weather_data(start_date: str, end_date: str) -> dict:
    """
    Fetch historical hourly weather data for NYC.
    """

    start = validate_date(start_date)
    end = validate_date(end_date)

    if start > end:
        raise ValueError(
            f"Start date {start_date} cannot be after end date {end_date}"
        )

    params = {
        "latitude": NYC_LATITUDE,
        "longitude": NYC_LONGITUDE,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": NYC_TIMEZONE,
    }

    logger.info(
        "Fetching NYC weather data from %s to %s",
        start_date,
        end_date,
    )

    response = requests.get(
        OPEN_METEO_URL,
        params=params,
        timeout=60,
    )

    logger.info(
        "Open-Meteo response status: %s",
        response.status_code,
    )

    response.raise_for_status()

    weather_data = response.json()

    return weather_data


def validate_weather_response(weather_data: dict) -> None:
    """
    Validate that the API returned the expected structure.
    """

    if not isinstance(weather_data, dict):
        raise ValueError("Weather API response is not a JSON object.")

    if weather_data.get("error") is True:
        reason = weather_data.get(
            "reason",
            "Unknown Open-Meteo API error",
        )
        raise ValueError(
            f"Open-Meteo API returned an error: {reason}"
        )

    required_top_level_keys = [
        "latitude",
        "longitude",
        "timezone",
        "hourly",
        "hourly_units",
    ]

    missing_keys = [
        key
        for key in required_top_level_keys
        if key not in weather_data
    ]

    if missing_keys:
        raise ValueError(
            f"Weather API response is missing keys: {missing_keys}"
        )

    hourly_data = weather_data["hourly"]

    if "time" not in hourly_data:
        raise ValueError(
            "Weather API response does not contain hourly time data."
        )

    for variable in HOURLY_VARIABLES:
        if variable not in hourly_data:
            raise ValueError(
                f"Weather API response is missing variable: {variable}"
            )

    # Make sure every hourly array has the same number of values.
    expected_length = len(hourly_data["time"])

    for variable in HOURLY_VARIABLES:
        actual_length = len(hourly_data[variable])

        if actual_length != expected_length:
            raise ValueError(
                f"Length mismatch for {variable}: "
                f"expected {expected_length}, "
                f"got {actual_length}"
            )

    logger.info(
        "Weather response validation successful. "
        "Hourly records: %s",
        expected_length,
    )

def get_month_ranges(
    start_date: str,
    end_date: str,
) -> list[tuple[str, str]]:
    """
    Split the requested date range into monthly date ranges.

    Example:
        2024-01-01 to 2024-02-29

    Returns:
        [
            ("2024-01-01", "2024-01-31"),
            ("2024-02-01", "2024-02-29"),
        ]
    """
    start = validate_date(start_date)
    end = validate_date(end_date)

    if start > end:
        raise ValueError(
            f"Start date {start_date} cannot be after end date {end_date}"
        )

    month_ranges = []

    current = start.replace(day=1)

    while current <= end:
        last_day = calendar.monthrange(
            current.year,
            current.month,
        )[1]

        month_start = max(
            start,
            current,
        )

        month_end = min(
            end,
            current.replace(day=last_day),
        )

        month_ranges.append(
            (
                month_start.strftime("%Y-%m-%d"),
                month_end.strftime("%Y-%m-%d"),
            )
        )

        if current.month == 12:
            current = current.replace(
                year=current.year + 1,
                month=1,
            )
        else:
            current = current.replace(
                month=current.month + 1,
            )

    return month_ranges

def build_file_name(start_date: str) -> str:
    """
    Build monthly weather filename.

    Example:
        2024-01-01 -> nyc_weather_2024-01.json
    """

    month = start_date[:7]

    return f"nyc_weather_{month}.json"

def build_raw_s3_key(etl_batch_id: str,
    year_month: str,
    file_name: str,
) -> str:
    """
    Build the S3 key for RAW weather data.

    Example:
        year_month = 2024-01
        file_name = nyc_weather_2024-01.json

    Returns:
        raw/weather/nyc/year=2024/month=01/nyc_weather_2024-01.json
    """
    year, month = year_month.split("-")

    return (
        f"raw/weather/nyc/"
        f"etl_batch_id={etl_batch_id}/"
        f"year={year}/"
        f"month={month}/"
        f"{file_name}"
    )

def save_weather_json(
    weather_data: dict,
    start_date: str,
    output_dir: Path,
) -> Path:
    """
    Save the original API response as JSON.
    """

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    file_name = build_file_name(start_date)

    output_path = output_dir / file_name

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            weather_data,
            file,
            indent=2,
        )

    logger.info(
        "Weather JSON saved to: %s",
        output_path,
    )

    return output_path



def upload_to_s3(
    local_file_path: Path,
    bucket_name: str,
    s3_key: str,
) -> None:
    s3_client = boto3.client("s3")

    logger.info(
        "Uploading %s to s3://%s/%s",
        local_file_path,
        bucket_name,
        s3_key,
    )

    s3_client.upload_file(
        str(local_file_path),
        bucket_name,
        s3_key,
    )

    logger.info(
        "Successfully uploaded to s3://%s/%s",
        bucket_name,
        s3_key,
    )

def read_etl_batch_id(source_system: str) -> str:
    """
    Read the ETL batch ID generated for the source system.
    """

    batch_id_file = (
        Path("parfiles")
        / source_system
        / "etl_batch_id.txt"
    )

    if not batch_id_file.exists():
        raise FileNotFoundError(
            f"ETL batch ID file not found: {batch_id_file}"
        )

    etl_batch_id = batch_id_file.read_text(
        encoding="utf-8"
    ).strip()

    if not etl_batch_id:
        raise ValueError(
            f"ETL batch ID file is empty: {batch_id_file}"
        )

    logger.info(
        "Using ETL batch ID: %s",
        etl_batch_id,
    )

    return etl_batch_id

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Fetch historical hourly NYC weather data "
            "from Open-Meteo."
        )
    )

    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format",
    )

    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format",
    )

    parser.add_argument(
    "--bucket-name",
    required=True,
    help="S3 bucket name",
    )

    parser.add_argument(
    "source_system",
    help="Source system name",
   )

    args = parser.parse_args()

    logger.info("Starting NYC weather ingestion")

    etl_batch_id = read_etl_batch_id(
    source_system=args.source_system
   )

    month_ranges = get_month_ranges(
        start_date=args.start_date,
        end_date=args.end_date,
    )

    logger.info(
        "Weather ingestion contains %s monthly range(s)",
        len(month_ranges),
    )

    for month_start, month_end in month_ranges:

        logger.info(
            "Processing weather data for %s to %s",
            month_start,
            month_end,
        )

        weather_data = fetch_weather_data(
            start_date=month_start,
            end_date=month_end,
        )

        validate_weather_response(weather_data)

        output_path = save_weather_json(
            weather_data=weather_data,
            start_date=month_start,
            output_dir=DEFAULT_OUTPUT_DIR,
        )

        year_month = month_start[:7]
        file_name = output_path.name

        raw_s3_key = build_raw_s3_key(
        etl_batch_id=etl_batch_id,
        year_month=year_month,
        file_name=file_name,
        )

        logger.info(
            "RAW S3 key: %s",
            raw_s3_key,
        )
        upload_to_s3(
            local_file_path=output_path,
            bucket_name=args.bucket_name,
            s3_key=raw_s3_key,
        )       
    logger.info("NYC weather ingestion completed successfully")

if __name__ == "__main__":
    main()


