# 7주차 과제 — Docker · AWS · Airflow · Spark · Git

- 이름: 이유석
- 기수: 데엔 3기

## 실습 환경
Windows 11 + Docker Desktop(WSL2) 위에서 Airflow 3.3.1 공식 docker-compose(CeleryExecutor) 와 apache/spark:3.5.3 Standalone 클러스터(master 1 + worker 2)로 실습했습니다.

## 회고
feature 브랜치 회고: XCom 으로 task 사이에 값을 넘기고, catchup 으로 과거 구간을 백필하는 동작을 직접 확인한 것이 가장 기억에 남습니다.
