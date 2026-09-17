from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def first_job():
    return "첫 번째 파이썬 작업 완료"


def second_job():
    return "두 번째 파이썬 작업 완료"


with DAG(
    dag_id="sample_dag",
    description="Q3 sample DAG: start -> python1 -> python2 -> end",
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
    tags=["week7", "Q3"],
) as dag:
    start = EmptyOperator(task_id="start")
    python_task_1 = PythonOperator(task_id="python_task_1", python_callable=first_job)
    python_task_2 = PythonOperator(task_id="python_task_2", python_callable=second_job)
    end = EmptyOperator(task_id="end")

    start >> python_task_1 >> python_task_2 >> end
