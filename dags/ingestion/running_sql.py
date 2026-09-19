from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="run_sql",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["run-sql-scripts"],
) as dag:
    create_hive_tables = BashOperator(
        task_id="create_hive_tables",
        bash_command=(
            "cd /opt/project && "
            "python generic_scripts/hive_sql_executor.py sql"
        ),
    )