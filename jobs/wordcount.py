"""Q4 — wordcount: data/wordcount.txt 를 읽어 공백 기준으로 단어를 나누고 빈도 상위 20개를 출력.

규칙: 분리 기준은 공백뿐. 소문자 변환·구두점 제거는 하지 않는다.
실행(컨테이너 안):
  spark-submit --master spark://spark-master:7077 /opt/jobs/wordcount.py /opt/data/wordcount.txt
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(input_path: str) -> None:
    spark = SparkSession.builder.appName("wordcount-yusuklee").getOrCreate()

    # 1) 텍스트 파일 읽기 (한 줄 = 한 행)
    lines = spark.read.text(input_path)

    # 2) 공백 기준으로 단어 분리 -> 한 단어 = 한 행 (빈 문자열 제외)
    words = (
        lines.select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
        .filter(F.col("word") != "")
    )

    # 3) 단어별 개수 집계
    counts = words.groupBy("word").count()

    total_words = words.count()
    unique_words = counts.count()
    print(f"총 단어 수: {total_words}, 고유 단어 수: {unique_words}")

    # 4) 빈도 내림차순 상위 20개 출력
    counts.orderBy(F.col("count").desc(), F.col("word")).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "/opt/data/wordcount.txt"
    main(path)
