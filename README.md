# 7주차 과제 — Docker · AWS · Airflow · Spark · Git

- 이름: 이유석
- 기수: 데엔 7기

## 실습 환경
Windows 11(RAM 8GB) + Docker Desktop(WSL2) 위에서 Airflow 3.3.1 공식 docker-compose(CeleryExecutor) 와 apache/spark:3.5.3 Standalone 클러스터(master 1 + worker 2)를 함께 띄워 실습했습니다.
S3 는 ap-northeast-2 리전의 IAM 사용자 키로 접근했고, 키는 코드가 아닌 자격증명 파일에만 두었습니다.

## 회고
XCom 으로 task 사이에 값을 넘기고 catchup 으로 과거 구간을 백필하는 동작을 직접 확인했습니다.
커스텀 Airflow 이미지를 빌드해 provider 를 추가하고, S3 → Spark → S3 로 이어지는 파이프라인을 DAG 하나로 묶어 본 것이 이번 주차에 배운 점입니다.
