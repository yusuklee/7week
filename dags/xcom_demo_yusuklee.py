import logging
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger("airflow.task")

TARGET_FILE = "/opt/airflow/data/wordcount.txt"


def count_lines(**context):
    ti = context["ti"]
    log.info(f"try_number = {ti.try_number}")
    if ti.try_number == 1:
        raise RuntimeError("첫 번째 시도는 일부러 실패시킵니다 (재시도 테스트)")
    with open(TARGET_FILE, encoding="utf-8") as f:
        n = sum(1 for _ in f)
    log.info(f"{TARGET_FILE} 의 줄 수 = {n}")
    return n


def report_lines(**context):
    ti = context["ti"]
    n = ti.xcom_pull(task_ids="count_lines")
    log.info(f"xcom_pull 로 받은 값: {n} (type={type(n).__name__})")
    log.info(f"wordcount.txt 는 총 {n} 줄입니다.")


with DAG(
    dag_id="xcom_demo_yusuklee",
    description="Q6 XCom + retry demo",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["week7", "Q6"],
) as dag:
    t1 = PythonOperator(
        task_id="count_lines",
        python_callable=count_lines,
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
    t2 = PythonOperator(task_id="report_lines", python_callable=report_lines)

    t1 >> t2
