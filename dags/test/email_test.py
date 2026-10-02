import logging
from datetime import datetime
import textwrap


from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import traceback


logger = logging.getLogger(__name__)


def intentional_failure():
    raise ValueError("This is an intentional test failure")


def failure_callback(context):
    failure_context = collect_failure_context(context)
    subject, body = build_email_content(failure_context)
    logger.error(subject)
    logger.error(body)
    
def collect_failure_context(context):
    ti = context["ti"]
    dag_run = context["dag_run"]
    exception = context.get("exception") 
    failure_context={ 
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": ti.run_id,
        "execution_date": str(dag_run.logical_date),
        "try_number": ti.try_number,
        "exception": str(exception) if exception else None,
        "traceback": "".join(traceback.TracebackException.from_exception(exception).format()),
    }
    return failure_context

def build_email_content(failure_context):
    subject = f"[Airflow Failure] {failure_context.get('dag_id')} | {failure_context.get('task_id')}"
    body = textwrap.dedent(f"""
        Hello Team,
        An Airflow task has failed. Please find the execution details below.
        1. Failure Details
        DAG: {failure_context.get("dag_id")}
        Task: {failure_context.get("task_id")}
        Run ID: {failure_context.get("run_id")}
        Execution Date: {failure_context.get("execution_date")}
        Attempt: {failure_context.get("try_number")}

        2. Error Information
        Exception:
        {failure_context.get("exception")}
        Traceback:
        {failure_context.get("traceback")}
    """).strip()
    
    return subject,body
          


with DAG(
    dag_id="email_notification_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    
) as dag:

    failing_task = PythonOperator(
        task_id="intentional_failure",
        python_callable=intentional_failure,
        on_failure_callback=failure_callback,
    )