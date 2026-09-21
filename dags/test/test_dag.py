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
    "owner" : "dataengineers",
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
    dag_id = "test_dag",
    default_args = default_args,
    description = 'DAG to find new users based on user location and country wise registration',
    schedule = '0 15 * * *',
    catchup = False,
 ) as dag:
    
    empty_operator_1 = EmptyOperator(task_id = 'start')
    #ingestion of user data
    batch_id_generation = BashOperator(
  
        task_id="batch_id_generation",
        bash_command="python /opt/project/generic_scripts/batch_id_generation.py cv1 landing",
    )
    parameter_generation = BashOperator(
      
            task_id="parameter_generation",
            bash_command="python /opt/project/generic_scripts/param_gen.py cv1",
        )
    landing_archival= BashOperator(
          
                task_id="landing_archival",
                bash_command="python /opt/project/generic_scripts/landing_archival.py cv1 dataforge_audit batch_log",
            )
    empty_operator_2 = EmptyOperator(task_id = 'end')
    
    
    # Dependencies
    empty_operator_1 >> batch_id_generation >> parameter_generation >> landing_archival >>  empty_operator_2


