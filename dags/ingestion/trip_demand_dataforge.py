from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum



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
    date_range_generation = BashOperator(
        task_id="date_range_generation",
        bash_command=(
            "cd /opt/project && "
            "python -m generic_scripts.date_range_generator "
        ),
    )

    fetch_source_files = BashOperator(
        task_id="fetch_source_files",
        bash_command=(
            "cd /opt/project && "
            "python generic_scripts/tlc_fetcher.py "
            "trip_demand_dataforge landing "
            "--taxi-type yellow && "
            "python generic_scripts/lookup_fetcher.py"
        ),
    )
    weather_ingestion = BashOperator(
        task_id="weather_ingestion",
        bash_command=(
            "cd /opt/project && "
            "python generic_scripts/weather_fetcher.py "
            "--bucket-name dataforge-lake "
            "nyc_weather"
        ),
    )
    
    
batch_id_generation >> date_range_generation >> fetch_source_files >> weather_batch_id_generation >> weather_ingestion
