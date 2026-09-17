import argparse
import csv
import os
import sys

import boto3

PREFIX = "bronze/"
FILE_NAME = "netflix_titles.csv"
LOCAL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def list_objects(s3, bucket: str, prefix: str) -> list:
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
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    print(f"[2] download  s3://{bucket}/{key} -> {os.path.relpath(local_path)}")
    s3.download_file(bucket, key, local_path)
    print(f"    downloaded ({os.path.getsize(local_path):,} bytes)")


def count_records(local_path: str) -> int:
    with open(local_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
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
