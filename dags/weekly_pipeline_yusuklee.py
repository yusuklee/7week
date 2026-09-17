import logging
import os
from datetime import datetime

from airflow.sdk import DAG, Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger("airflow.task")

FILE_NAME = "netflix_titles.csv"
BRONZE_KEY = f"bronze/{FILE_NAME}"
LOCAL_CSV = f"/opt/airflow/data/{FILE_NAME}"
OUTPUT_DIR = "/opt/airflow/output/silver"
JOB_PATH = "/opt/airflow/dags/jobs/transform.py"


def download_from_s3(**context):
    bucket = Variable.get("netflix_bucket")
    s3 = S3Hook(aws_conn_id="aws_default").get_conn()
    os.makedirs(os.path.dirname(LOCAL_CSV), exist_ok=True)
    s3.download_file(bucket, BRONZE_KEY, LOCAL_CSV)
    size = f"{os.path.getsize(LOCAL_CSV):,}"
    log.info("downloaded s3://%s/%s -> %s (%s bytes)", bucket, BRONZE_KEY, LOCAL_CSV, size)
    return LOCAL_CSV


def upload_to_s3(**context):
    bucket = Variable.get("netflix_bucket")
    today = datetime.now().strftime("%Y-%m-%d")
    run_date = context["dag_run"].run_after.strftime("%Y-%m-%d")
    local_dir = os.path.join(OUTPUT_DIR, run_date)
    prefix = f"silver/{today}/"
    s3 = S3Hook(aws_conn_id="aws_default").get_conn()

    uploaded = 0
    for name in sorted(os.listdir(local_dir)):
        path = os.path.join(local_dir, name)
        if os.path.isfile(path) and not name.endswith(".crc"):
            s3.upload_file(path, bucket, prefix + name)
            uploaded += 1
    log.info("uploaded %d files -> s3://%s/%s", uploaded, bucket, prefix)

    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objs = resp.get("Contents", [])
    for o in objs:
        log.info("  %s  %s bytes", o["Key"], f"{o['Size']:,}")
    log.info("silver/%s objects: %d", today, len(objs))
    return len(objs)


with DAG(
    dag_id="weekly_pipeline_yusuklee",
    description="Q9 S3 -> Spark -> S3 pipeline",
    start_date=datetime(2026, 9, 1),
    schedule="@weekly",
    catchup=False,
    tags=["week7", "Q9"],
) as dag:
    download = PythonOperator(task_id="download_from_s3", python_callable=download_from_s3)

    transform = BashOperator(
        task_id="transform_with_spark",
        bash_command=(
            f"spark-submit --master local[*] {JOB_PATH} "
            f"--input {LOCAL_CSV} --output {OUTPUT_DIR}/{{{{ dag_run.run_after.strftime('%Y-%m-%d') }}}} "
            "--min-year {{ params.min_year }}"
        ),
        params={"min_year": 2015},
    )

    upload = PythonOperator(task_id="upload_to_s3", python_callable=upload_to_s3)

    download >> transform >> upload
