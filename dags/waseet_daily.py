"""The daily scan DAG.

A skeleton with the imports you will need and the shape of the run. Fill in the
tasks, wire the dependencies, and delete the comments as you replace them.

Inside the container the paths are:

    /opt/airflow/data       the scan files and lookups
    /opt/airflow/pipeline   pipeline.py and quality.py

Iterate with `dags test`, which runs the whole DAG in one process and prints
straight to your terminal - no unpausing, no waiting for the scheduler:

    docker exec waseet-airflow airflow dags test waseet_daily 2026-05-04
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.filesystem import FileSensor
import pendulum

DATA = "/opt/airflow/data"
CODE = "/opt/airflow/pipeline"

def alert(context):
    task = context["task_instance"].task_id
    ds = context["ds"]
    line = f"{ds} {task} failed\n"
    print("ALERT:", line)

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert,
}

def choose_branch(ds):

    import pandas as pd
    file_path = f"{DATA}/scans_{ds}.csv"
    try:
        df = pd.read_csv(file_path, dtype=str)
        if len(df) == 0:
            return "skip_day"
    except Exception:
        return "skip_day"
    return "run_pipeline"


with DAG(
    dag_id="waseet_daily",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["waseet"],
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=DATA + "/scans_{{ ds }}.csv",
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=60,
        mode="reschedule",
    )

    choose = BranchPythonOperator(
        task_id="choose",
        python_callable=choose_branch,
    )    
    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command=f"cd {CODE} && python3 pipeline.py --date {{{{ ds }}}}"
    )
    skip_day = BashOperator(
        task_id="skip_day",
        bash_command="echo no rows for {{ ds }}, nothing to load",
    ) 
    run_quality = BashOperator(
        task_id="run_quality",
        bash_command=f"cd {CODE} && python quality.py --date {{{{ ds }}}}"
    )
    finish = BashOperator(
        task_id="finish",
        bash_command="echo {{ ds }} done",
        trigger_rule="none_failed_min_one_success",
    )
    wait_for_file >> choose >> [run_pipeline, skip_day]
    run_pipeline >> run_quality >> finish
    skip_day >> finish  