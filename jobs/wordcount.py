import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(input_path: str) -> None:
    spark = SparkSession.builder.appName("wordcount-yusuklee").getOrCreate()

    lines = spark.read.text(input_path)

    words = (
        lines.select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
        .filter(F.col("word") != "")
    )

    counts = words.groupBy("word").count()

    total_words = words.count()
    unique_words = counts.count()
    print(f"총 단어 수: {total_words}, 고유 단어 수: {unique_words}")

    counts.orderBy(F.col("count").desc(), F.col("word")).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "/opt/data/wordcount.txt"
    main(path)
