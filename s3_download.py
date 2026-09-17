"""Q8 — boto3 로 S3 bronze/ 객체 목록·크기 출력 -> netflix_titles.csv 다운로드 -> CSV 레코드 수 출력.

버킷 이름은 코드에 적지 않고 환경변수(S3_BUCKET) 또는 실행 인자로 받는다.
액세스 키는 코드에 적지 않는다. (aws configure 로 설정한 자격증명 파일을 boto3 가 자동으로 읽는다)

실행 예:
  S3_BUCKET=de-3-yusuklee python s3_download.py
  python s3_download.py --bucket de-3-yusuklee
"""
import argparse
import csv
import os
import sys

import boto3

PREFIX = "bronze/"
FILE_NAME = "netflix_titles.csv"
LOCAL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def list_objects(s3, bucket: str, prefix: str) -> list:
    """[1] prefix 아래 객체 목록과 각 객체의 크기를 출력한다."""
    print(f"[1] list  s3://{bucket}/{prefix}")
    objs = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objs.append(obj)
            print(f"    {obj['Key']:<45} {obj['Size']:>12,} bytes")
    if not objs:
        print("    (객체 없음)")
    return objs


def download(s3, bucket: str, key: str, local_path: str) -> None:
    """[2] 객체를 로컬로 다운로드한다."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    print(f"[2] download  s3://{bucket}/{key} -> {os.path.relpath(local_path)}")
    s3.download_file(bucket, key, local_path)
    print(f"    downloaded ({os.path.getsize(local_path):,} bytes)")


def count_records(local_path: str) -> int:
    """[3] CSV 레코드 수를 센다. 헤더 제외, 따옴표 안의 줄바꿈은 한 행으로 취급(csv 모듈 사용)."""
    with open(local_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)                  # 헤더 제외
        n = sum(1 for _ in reader)
    print(f"[3] rows  {n:,}  (header: {len(header)} columns, header excluded)")
    return n


def main() -> int:
    parser = argparse.ArgumentParser(description="S3 bronze/ 목록 · 다운로드 · 레코드 수")
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET"),
                        help="S3 버킷 이름 (환경변수 S3_BUCKET 으로도 지정 가능)")
    parser.add_argument("--region", default=os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))
    args = parser.parse_args()
    if not args.bucket:
        print("버킷 이름이 없습니다. --bucket 또는 환경변수 S3_BUCKET 을 지정하세요.", file=sys.stderr)
        return 1

    s3 = boto3.client("s3", region_name=args.region)
    key = PREFIX + FILE_NAME
    local_path = os.path.join(LOCAL_DIR, FILE_NAME)

    list_objects(s3, args.bucket, PREFIX)
    download(s3, args.bucket, key, local_path)
    count_records(local_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
