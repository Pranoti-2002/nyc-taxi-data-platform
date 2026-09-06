import argparse
from datetime import datetime
import requests
from pathlib import Path
import pyarrow.parquet as pq 

TAXI_FILE_PREFIX = {
    "yellow": "yellow_tripdata",
    "green": "green_tripdata",
    "fhv": "fhv_tripdata",
    "hvfhv": "fhvhv_tripdata",
}

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

    response = requests.get(url, stream=True, timeout=60)  # Set a timeout for the request
    response.raise_for_status()  # Raise an error for bad responses

    with open(local_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size = 1024*1024):  # Download in chunks of 1MB
            if chunk:  # Filter out keep-alive new chunks
                file.write(chunk)

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

    print(
        f"Validated: {local_path} "
        f"({local_path.stat().st_size} bytes)"
    )

def validate_parquet(local_path):
    local_path = Path(local_path)

    try:
        parquet_file = pq.ParquetFile(local_path)

        if parquet_file.metadata.num_rows == 0:
            raise ValueError(
                f"Parquet file contains no rows: {local_path}"
            )

        print(
            f"Parquet validation passed: "
            f"{local_path} "
            f"({parquet_file.metadata.num_rows} rows)"
        )

    except Exception as error:
        raise ValueError(
            f"Invalid Parquet file: {local_path}"
        ) from error

def main():
    args = parse_arguments()
    taxi_type = args.taxi_type
    start_date = args.start_date
    end_date = args.end_date

    # Here you would add the logic to download the data based on the provided arguments
    #print(f"Downloading {taxi_type} taxi data from {start_date} to {end_date}")

    months = generate_months(start_date, end_date)
    #print(f"Months to download: {months}")

    for year_month in months:
        url = build_tlc_url(taxi_type, year_month)
        #print(f"URL for {year_month}: {url}")
        file_name = url.split("/")[-1] 
        local_path = Path(f"data/bronze/{file_name}")
        download_file(url, local_path)
        validate_download(local_path)
        validate_parquet(local_path)
if __name__ == "__main__":
    main()