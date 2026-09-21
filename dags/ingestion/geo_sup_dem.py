from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum
from airflow.operators.empty import EmptyOperator


# Define the local timezone
local_tz = pendulum.timezone("Asia/Kolkata")

# Default Args
default_args = {
    "owner" : "HorizonChaser12",
    "depends_on_past" : False,
    "email_on_failure" : True,
    "email_on_retry" : False,
    "email" : "suryakant.mangaraj@gmail.com",
    # "retries" : 1,
    "max_active_runs" : 5,
    "dagrun_timeout" : timedelta(hours=1),
    "start_date" : datetime(2026, 5, 1, tzinfo=local_tz),
    # "end_date" : 
}

with DAG(
    dag_id = "geo_sup_dem_ingestion",
    default_args = default_args,
    description = 'Ingestion Layer for Business case 6 for TLC Data',
    schedule = '0 15 * * *',
    catchup = False,
 ) as dag:
    
    empty_operator_1 = EmptyOperator(task_id = 'start')
    batch_id_generation_extraction = BashOperator(
        task_id="batch_id_generation_extraction",
        bash_command="python /opt/project/generic_scripts/batch_id_generation_extraction.py geo_sup_bs_6 landing",
    )
    parameter_generation = BashOperator(
        task_id="parameter_generation",
        bash_command="python /opt/project/generic_scripts/param_gen.py geo_sup_bs_6",
    )
    tlc_taxi_data_downloader_extraction_to_s3 = BashOperator(
        task_id="tlc_yellow_taxi_data_downloader_extraction_to_s3",
        bash_command=(
            "python /opt/project/generic_scripts/tlc_fetcher.py "
            "--taxi-type yellow green fhv hvfhv "
            "--start-date 2024-01 "
            "--end-date 2024-01"
        ),
    )
    tlc_taxi_data_lookup_downloader_extraction_to_s3 = BashOperator(
        task_id="tlc_taxi_data_lookup_downloader_extraction_to_s3",
        bash_command=(
            "python /opt/project/generic_scripts/lookup_fetcher.py"
        )
    )
    landing_archival = BashOperator(
        task_id="landing_archival",
        bash_command=(
            "python /opt/project/generic_scripts/landing_archival.py geo_sup_bs_6 dataforge_landing yellow_taxi && "
            "python /opt/project/generic_scripts/landing_archival.py geo_sup_bs_6 dataforge_landing green_taxi && "
            "python /opt/project/generic_scripts/landing_archival.py geo_sup_bs_6 dataforge_landing fhv_trips && "
            "python /opt/project/generic_scripts/landing_archival.py geo_sup_bs_6 dataforge_landing fhvhv_trips"
        ),
    )
    empty_operator_2 = EmptyOperator(task_id = 'end')
    
    
    # Dependencies
    empty_operator_1 >> batch_id_generation_extraction >> parameter_generation >> tlc_taxi_data_downloader_extraction_to_s3 >> tlc_taxi_data_lookup_downloader_extraction_to_s3 >> landing_archival >>  empty_operator_2


