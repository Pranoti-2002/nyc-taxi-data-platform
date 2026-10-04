import logging
from google import genai
from google.genai import types
from pydantic import BaseModel
import os
from airflow.utils.email import send_email_smtp
import traceback


logger = logging.getLogger(__name__)


def failure_callback(email):
    def callback(context):
        failure_context = collect_failure_context(context)
        ai_analysis = analyze_failure_with_gemini(failure_context)
        subject, body = build_email_content(failure_context, ai_analysis)
        send_email_smtp(
            to=email,
            subject=subject,
            html_content=body,
        )
    return callback    

def collect_task_logs(ti):
    log_path = (
        f"/opt/airflow/logs/"
        f"dag_id={ti.dag_id}/"
        f"run_id={ti.run_id}/"
        f"task_id={ti.task_id}/"
        f"attempt={ti.try_number}.log"
    )

    try:
        with open(log_path, "r") as log_file:
            return log_file.read()

    except Exception as e:
        logger.warning(
            f"Unable to read task log from {log_path}: {e}"
        )
        return None

def build_email_content(failure_context, ai_analysis):
    subject = (
        f"[Airflow Failure] "
        f"{failure_context.get('dag_id')} | "
        f"{failure_context.get('task_id')}"
    )

    body = f"""
    <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6;">

            <h2>Airflow DAG Failure</h2>

            <h3>Failure Details</h3>

            <table border="1" cellpadding="8" cellspacing="0"
                   style="border-collapse: collapse;">
                <tr>
                    <td><b>DAG</b></td>
                    <td>{failure_context.get("dag_id")}</td>
                </tr>

                <tr>
                    <td><b>Task</b></td>
                    <td>{failure_context.get("task_id")}</td>
                </tr>

                <tr>
                    <td><b>Run ID</b></td>
                    <td>{failure_context.get("run_id")}</td>
                </tr>

                <tr>
                    <td><b>Execution Date</b></td>
                    <td>{failure_context.get("execution_date")}</td>
                </tr>

                <tr>
                    <td><b>Attempt</b></td>
                    <td>{failure_context.get("try_number")}</td>
                </tr>
            </table>

            <h3>Error Information</h3>

            <p><b>Exception:</b></p>

            <pre style="background-color:#f4f4f4;
                        padding:10px;
                        border-radius:5px;">
            {failure_context.get("exception")}
            </pre>

            <p><b>Traceback:</b></p>

            <pre style="background-color:#f4f4f4;
                        padding:10px;
                        border-radius:5px;
                        overflow-x:auto;">
            {failure_context.get("traceback")}
            </pre>

            <h3>AI Analysis</h3>

            <p><b>Root Cause:</b></p>
            <p>{ai_analysis.get("root_cause")}</p>

            <p><b>Evidence:</b></p>
            <p>{ai_analysis.get("evidence")}</p>

            <p><b>Suggested Fix:</b></p>
            <p>{ai_analysis.get("suggested_fix")}</p>

            <p><b>Confidence:</b>
                {ai_analysis.get("confidence")}
            </p>

            <hr>

            <p style="font-size:12px; color:gray;">
                This notification was generated automatically by the
                Airflow AI Failure Notification Framework.
            </p>

        </body>
    </html>
    """

    return subject, body
    
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
        "traceback": (
            "".join(traceback.TracebackException.from_exception(exception).format())
            if exception
            else None
        ),
        "task_logs": collect_task_logs(ti),
    }
    return failure_context

class FailureAnalysis(BaseModel):
    root_cause: str
    evidence: str
    suggested_fix: str
    confidence: str

def analyze_failure_with_gemini(failure_context):
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

    prompt = f"""
    You are an Airflow failure-analysis assistant.

    Analyze the Airflow failure information provided below.

    Your responsibilities:
    1. Identify the root cause only when it is supported by the available evidence.
    2. Explain the evidence that supports the conclusion.
    3. Suggest a practical fix or next troubleshooting step.
    4. Assign confidence as one of: high, medium, low, unknown.

    IMPORTANT RULES:
    - Analyze ONLY the information provided below.
    - Do not invent missing code, logs, configuration, infrastructure,
      dependencies, or system behavior.
    - Do not assume the cause when the evidence is insufficient.
    - If the root cause cannot be determined, explicitly say:
      "Unable to determine the root cause from the available information."
    - A suggested fix should be based on the available evidence.
    - If the failure appears intentional, state that clearly.

    AIRFLOW FAILURE INFORMATION:

    DAG ID:
    {failure_context.get("dag_id")}

    TASK ID:
    {failure_context.get("task_id")}

    RUN ID:
    {failure_context.get("run_id")}

    EXECUTION DATE:
    {failure_context.get("execution_date")}

    TRY NUMBER:
    {failure_context.get("try_number")}

    EXCEPTION:
    {failure_context.get("exception")}

    TRACEBACK:
    {failure_context.get("traceback")}
    
    TASK LOGS:
    {failure_context.get("task_logs")}
    """

    response = client.models.generate_content(
        model="gemini-3.5-flash",
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=FailureAnalysis,
        ),
    )

    return response.parsed.model_dump()