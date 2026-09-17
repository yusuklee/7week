# Q5 — 공식 Airflow 이미지를 베이스로 JDK 와 파이썬 패키지(AWS · Spark provider, pyspark)를 추가한 커스텀 이미지
FROM apache/airflow:3.3.1

# 1) JDK 설치 — apt 는 root 로 실행해야 한다
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 2) 파이썬 패키지 설치 — pip 는 반드시 airflow 사용자로 실행한다
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
