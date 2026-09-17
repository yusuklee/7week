"""Q7 — 백필(backfill) 데모 DAG.

start_date 를 작업일(2026-09-17) 기준 7일 전인 2026-09-10 으로 고정하고 catchup=True 로 두어,
DAG 를 켰을 때 과거 구간의 scheduled 실행이 자동으로 채워지는 것을 확인한다.
각 실행은 자신의 logical date(ds) 이름이 붙은 파일을 /opt/airflow/output/backfill/ 아래에 쓴다.
"""
import logging
from datetime import datetime
import os

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger("airflow.task")

OUTPUT_DIR = "/opt/airflow/output/backfill"


def write_daily_file(**context):
    ds = context["ds"]                                  # 실행의 logical date (YYYY-MM-DD)
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
    start_date=datetime(2026, 9, 10),      # 작업일 7일 전 날짜 리터럴로 고정
    schedule="@daily",                     # 매일 1회
    catchup=True,                          # 과거 구간 실행을 자동으로 채운다
    max_active_runs=3,
    tags=["week7", "Q7"],
) as dag:
    PythonOperator(task_id="write_daily_file", python_callable=write_daily_file)
