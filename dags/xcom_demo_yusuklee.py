"""Q6 — XCom 전달 + 재시도 데모 DAG.

앞 작업(count_lines)은 파일의 줄 수를 계산해 return 하고(XCom return_value 로 저장),
뒤 작업(report_lines)은 xcom_pull 로 그 값을 받아 로그에 출력한다.
앞 작업은 retries=2, retry_delay=30초 로 설정하고, 첫 번째 시도(try_number == 1)에서만 일부러 실패시켜
재시도로 성공하는 것을 확인한다.
"""
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
        # 첫 시도에서만 실패 -> 재시도로 성공하게 만든다
        raise RuntimeError("첫 번째 시도는 일부러 실패시킵니다 (재시도 테스트)")
    with open(TARGET_FILE, encoding="utf-8") as f:
        n = sum(1 for _ in f)
    log.info(f"{TARGET_FILE} 의 줄 수 = {n}")
    return n            # XCom (return_value) 에 저장


def report_lines(**context):
    ti = context["ti"]
    n = ti.xcom_pull(task_ids="count_lines")     # 앞 작업의 반환값을 받는다
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
        retries=2,                              # 2회 이상 재시도
        retry_delay=timedelta(seconds=30),
    )
    t2 = PythonOperator(task_id="report_lines", python_callable=report_lines)

    t1 >> t2
