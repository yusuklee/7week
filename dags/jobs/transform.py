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
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.csv(args.input, header=True, inferSchema=False, multiLine=True, escape='"')
    total = df.count()

    filtered = (
        df.withColumn("release_year", F.col("release_year").cast("int"))
          .filter(F.col("release_year") >= args.min_year)
    )
    kept = filtered.count()

    exploded = (
        filtered.withColumn("genre", F.explode(F.split(F.col("listed_in"), ",")))
                .withColumn("genre", F.trim(F.col("genre")))
                .filter(F.col("genre") != "")
    )

    agg = (
        exploded.groupBy("type", "genre")
                .agg(F.count("*").alias("title_count"))
                .orderBy("type", F.col("title_count").desc(), "genre")
    )

    agg_rows = agg.count()
    print(f"input rows={total}, rows with release_year>={args.min_year}: {kept}")
    print(f"AGG_ROW_COUNT={agg_rows}   (type x genre 집계 행 수)")

    agg.write.mode("overwrite").option("compression", "snappy").parquet(args.output)
    print(f"saved parquet(snappy) -> {args.output}")

    agg.show(agg_rows, truncate=False)
    print(f"AGG_ROW_COUNT={agg_rows}   (type x genre 집계 행 수 — 최종)")
    spark.stop()


if __name__ == "__main__":
    main()
