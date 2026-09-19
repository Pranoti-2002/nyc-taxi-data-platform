from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="nyc_taxi_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "ingestion"],
) as dag:
    batch_id_generation = BashOperator(
        task_id="batch_id_generation",
        bash_command=(
            "cd /opt/project && "
            "python -m generic_scripts.batch_id_generation "
            "nyc_taxi landing"
        ),
    )
    weather_batch_id_generation = BashOperator(
        task_id="weather_batch_id_generation",
        bash_command=(
            "cd /opt/project && "
            "python -m generic_scripts.batch_id_generation "
            "nyc_weather landing"
        ),
    )
    prm_generation = BashOperator(
    task_id="prm_generation",   
    bash_command=(
        "cd /opt/project && "
        "python -m generic_scripts.param_gen nyc_taxi"
        ),
    )
    fetch_source_files = BashOperator(
        task_id="fetch_source_files",
        bash_command=(
            "cd /opt/project && "
            "python generic_scripts/tlc_fetcher.py  "
            "--taxi-type yellow "
            "--start-date 2024-01 "
            "--end-date 2024-01 "
            "nyc_taxi landing && "
            "python generic_scripts/lookup_fetcher.py"
        ),
    )
    weather_ingestion = BashOperator(
        task_id="weather_ingestion",
        bash_command=(
            "cd /opt/project && "
            "python generic_scripts/weather_fetcher.py "
            "--start-date 2024-01-01 "
            "--end-date 2024-02-29 "
            "--bucket-name dataforge-lake "
            "nyc_weather"
        ),
    )
    
    
batch_id_generation >> prm_generation >> fetch_source_files >> weather_batch_id_generation >> weather_ingestion
