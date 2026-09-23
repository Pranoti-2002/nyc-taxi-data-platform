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

    inspect_tlc_parquet_schemas = BashOperator(
        task_id="inspect_tlc_parquet_schemas",
        bash_command=(
            "cd /opt/project && "
            "python generic_scripts/schema_inspector.py "
            "/opt/project/data/bronze/yellow_tripdata_2024-01.parquet "
            "/opt/project/data/bronze/green_tripdata_2024-01.parquet "
            "/opt/project/data/bronze/fhv_tripdata_2024-01.parquet "
            "/opt/project/data/bronze/fhvhv_tripdata_2024-01.parquet "
            "--output-file /opt/project/data/schema_outputs/tlc_schemas.txt"
        ),
    )

    inspect_lookup_csv_schema = BashOperator(
        task_id="inspect_lookup_csv_schema",
        bash_command=(
            "cd /opt/project && "
            "python generic_scripts/schema_inspector.py "
            "data/bronze/lookup/taxi_zone_lookup.csv "
            "--output-file /opt/project/data/schema_outputs/lookup_schema.txt"
        ),
    )