import logging
from datetime import datetime
import os

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger("airflow.task")

OUTPUT_DIR = "/opt/airflow/output/backfill"


def write_daily_file(**context):
    ds = context["ds"]
    logical_date = context["logical_date"]
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"daily_{ds}.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"logical_date={logical_date.isoformat()}\n")
        f.write(f"run_id={context['run_id']}\n")
        f.write(f"written_at={datetime.now().isoformat()}\n")
    log.info(f"wrote {path}")
    return path


with DAG(
    dag_id="backfill_demo_yusuklee",
    description="Q7 backfill demo (catchup=True)",
    start_date=datetime(2026, 9, 10),
    schedule="@daily",
    catchup=True,
    max_active_runs=3,
    tags=["week7", "Q7"],
) as dag:
    PythonOperator(task_id="write_daily_file", python_callable=write_daily_file)
