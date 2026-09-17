"""Q9 — Spark 집계 job: netflix_titles.csv -> release_year 필터 -> listed_in 장르 explode -> type x 장르 집계 -> parquet(snappy).

실행 예:
  spark-submit transform.py --input /opt/airflow/data/netflix_titles.csv --output /opt/airflow/output/silver/2026-09-17 --min-year 2015
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="입력 CSV 경로 (컨테이너 안 로컬 경로)")
    parser.add_argument("--output", required=True, help="parquet 출력 경로 (컨테이너 안 로컬 경로)")
    parser.add_argument("--min-year", type=int, default=2015, help="기준 연도 (이 값 이상만 남김, 기본 2015)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("netflix-transform-yusuklee")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")   # 집계 결과가 잘 보이도록 INFO 로그를 줄인다

    # 따옴표 안의 줄바꿈이 있는 CSV 이므로 multiLine 옵션으로 읽는다
    df = spark.read.csv(args.input, header=True, inferSchema=False, multiLine=True, escape='"')
    total = df.count()

    # 1) release_year 가 기준 연도 이상인 행만 남긴다
    filtered = (
        df.withColumn("release_year", F.col("release_year").cast("int"))
          .filter(F.col("release_year") >= args.min_year)
    )
    kept = filtered.count()

    # 2) listed_in(쉼표 구분 장르 목록) -> 장르 하나당 한 행, 앞뒤 공백 제거
    exploded = (
        filtered.withColumn("genre", F.explode(F.split(F.col("listed_in"), ",")))
                .withColumn("genre", F.trim(F.col("genre")))
                .filter(F.col("genre") != "")
    )

    # 3) type x 장르 별 작품 수 집계
    agg = (
        exploded.groupBy("type", "genre")
                .agg(F.count("*").alias("title_count"))
                .orderBy("type", F.col("title_count").desc(), "genre")
    )

    agg_rows = agg.count()
    print(f"input rows={total}, rows with release_year>={args.min_year}: {kept}")
    print(f"AGG_ROW_COUNT={agg_rows}   (type x genre 집계 행 수)")

    # 4) parquet(snappy) 로 저장 (컨테이너 안 로컬 경로)
    agg.write.mode("overwrite").option("compression", "snappy").parquet(args.output)
    print(f"saved parquet(snappy) -> {args.output}")

    agg.show(agg_rows, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
