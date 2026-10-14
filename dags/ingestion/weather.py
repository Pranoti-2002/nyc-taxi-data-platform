from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator




with DAG(
    dag_id="weather_dataforge_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "ingestion"],
) as dag:
    start = EmptyOperator(task_id="start")
    weather_batch_id_generation = BashOperator(
        task_id="weather_batch_id_generation",
        bash_command=(
            "cd /opt/project && "
            "python -m generic_scripts.batch_id_generation_extraction "
            "nyc_weather landing"
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
    end = EmptyOperator(task_id="end")

start>>weather_batch_id_generation >> weather_ingestion >> end