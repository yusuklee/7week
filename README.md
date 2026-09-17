# 7주차 과제 — Docker · AWS · Airflow · Spark · Git

- 이름: 이유석
- 기수: 데엔 3기

## 실습 환경
로컬 PC(Windows 11, RAM 8GB) 의 Docker Desktop 에서 Airflow 와 Spark 컨테이너를 함께 띄워 실습했고, S3 는 ap-northeast-2 리전의 IAM 사용자 키로 접근했습니다.

## 회고
master 회고: 커스텀 Airflow 이미지를 빌드해 provider 를 추가하고, S3 → Spark → S3 로 이어지는 파이프라인을 DAG 하나로 묶어 본 것이 이번 주차에 배운 점입니다.
