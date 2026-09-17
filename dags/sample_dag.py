"""Q3 — 시작 → 파이썬 작업 2개 → 종료 4개 task 를 순차 연결하는 샘플 DAG."""
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def first_job():
    # print 가 아니라 문자열을 return 합니다 (XCom return_value 로 저장됨)
    return "첫 번째 파이썬 작업 완료"


def second_job():
    return "두 번째 파이썬 작업 완료"


with DAG(
    dag_id="sample_dag",
    description="Q3 sample DAG: start -> python1 -> python2 -> end",
    start_date=datetime(2026, 9, 1),
    schedule="@daily",   # 매일 1회
    catchup=False,       # 과거 구간 자동 실행 안 함
    tags=["week7", "Q3"],
) as dag:
    start = EmptyOperator(task_id="start")
    python_task_1 = PythonOperator(task_id="python_task_1", python_callable=first_job)
    python_task_2 = PythonOperator(task_id="python_task_2", python_callable=second_job)
    end = EmptyOperator(task_id="end")

    start >> python_task_1 >> python_task_2 >> end
