from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="trip_demand_dataforge_ingestion",
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
            "trip_demand_dataforge landing"
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
            "python -m generic_scripts.param_gen trip_demand_dataforge"
        ),
    )
    fetch_source_files = BashOperator(
        task_id="fetch_source_files",
        bash_command=(
            "cd /opt/project && "
            "python generic_scripts/tlc_fetcher.py  "
            "--taxi-type yellow "
            "--start-date {{ logical_date.start_of('month').subtract(months=3).strftime('%Y-%m') }} "
            "--end-date {{ logical_date.start_of('month').subtract(months=1).strftime('%Y-%m') }} "
            "trip_demand_dataforge landing && "
            "python generic_scripts/lookup_fetcher.py"
        ),
    )
    weather_ingestion = BashOperator(
        task_id="weather_ingestion",
        bash_command=(
            "cd /opt/project && "
            "python generic_scripts/weather_fetcher.py "
            "--start-date {{ logical_date.start_of('month').subtract(months=3).strftime('%Y-%m-%d') }} "
            "--end-date {{ logical_date.start_of('month').subtract(months=1).strftime('%Y-%m-%d') }} "
            "--bucket-name dataforge-lake "
            "nyc_weather"
        ),
    )
    
    
batch_id_generation >> prm_generation >> fetch_source_files >> weather_batch_id_generation >> weather_ingestion
