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

    download_yellow_taxi = BashOperator(
        task_id="download_yellow_taxi",
        bash_command=(
            "cd /opt/project && "
            "python generic_scripts/tlc_fetcher.py "
            "--taxi-type yellow "
            "--start-date 2024-01 "
            "--end-date 2024-01"
        ),
    )