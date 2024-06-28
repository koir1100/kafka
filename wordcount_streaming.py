from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Streaming Word Count") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()

    # READ
    lines_df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "9999") \
        .load()

    # TRANSFORM
    words_df = lines_df.select(expr("explode(split(value,' ')) as word"))
    # counts_df = words_df.groupBy("word").count()

    words_df.createOrReplaceTempView("words")
    counts_df = spark.sql("""SELECT word, COUNT(word) AS count FROM words GROUP BY word ORDER BY count DESC""")

    # SINK
    word_count_query = counts_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("checkpointLocation", "chk-point-dir") \
        .start()

    print("Listening to localhost:9999")
    word_count_query.awaitTermination()
