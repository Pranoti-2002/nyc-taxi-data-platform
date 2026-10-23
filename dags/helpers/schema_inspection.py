from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="source_schema_inspection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["helper", "schema-inspection"],
) as dag:
    inspect_tlc_parquet_schemas = BashOperator(
        task_id="inspect_tlc_parquet_schemas",
        bash_command=(
            "cd /opt/project && "
            "python generic_scripts/schema_inspector.py "
            "/opt/project/data/bronze/yellow_tripdata_2020-01.parquet "
            "--output-file /opt/project/data/schema_outputs/tlc_schemas.txt"
        ),
    )

    inspect_lookup_csv_schema = BashOperator(
        task_id="inspect_lookup_csv_schema",
        bash_command=(
            "cd /opt/project && "
            "python generic_scripts/schema_inspector.py "
            "/opt/project/data/bronze/lookup/taxi_zone_lookup.csv "
            "--output-file /opt/project/data/schema_outputs/lookup_schema.txt"
        ),
    )
