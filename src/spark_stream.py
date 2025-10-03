import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, length, lit, to_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, TimestampType

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "mastodon_stream"

JDBC_URL = "jdbc:postgresql://pg:5432/mastodon"
JDBC_PROPS = {"user": "mastodon", "password": "mastodon", "driver": "org.postgresql.Driver"}

FILTER_LANGUAGE = None
FILTER_KEYWORDS = []

schema = StructType([
    StructField("id", LongType(), True),
    StructField("created_at", StringType(), True),
    StructField("language", StringType(), True),
    StructField("text", StringType(), True),
    StructField("hashtags", ArrayType(StringType()), True),
    StructField("user_id", LongType(), True),
    StructField("username", StringType(), True),
    StructField("favourites", LongType(), True),
    StructField("reblogs", LongType(), True),
    StructField("replies", LongType(), True),
    StructField("url", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("MastodonStreamProcessing")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

df = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .select(from_json(col("json"), schema).alias("data"))
       .select("data.*")
)

# Normalisation du timestamp
df = df.withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss.SSSSSSXXX"))

filtered_df = df

if FILTER_LANGUAGE:
    filtered_df = filtered_df.filter(col("language") == FILTER_LANGUAGE)

if FILTER_KEYWORDS:
    keyword_pattern = "(?i)(" + "|".join(re.escape(word) for word in FILTER_KEYWORDS) + ")"
    filtered_df = filtered_df.filter(col("text").isNotNull() & col("text").rlike(keyword_pattern))


def sink(batch_df, batch_id: int):
    # On garde ici uniquement les toots avec timestamp + texte valides pour les actions
    base = batch_df.filter(col("created_at").isNotNull() & col("text").isNotNull())

    # Action 1: nombre de toots par fenêtre de 1 minute
    toots_per_minute = (
        base.groupBy(window(col("created_at"), "1 minute"))
            .count()
            .select(
                lit(batch_id).alias("batch_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("count").alias("cnt")
            )
    )
    toots_per_minute.write.mode("append").jdbc(JDBC_URL, "streamed_toot_counts", properties=JDBC_PROPS)

    # Action 2: longueur moyenne par utilisateur
    avg_length_per_user = (
        base.withColumn("length", length(col("text")))
            .groupBy("username")
            .avg("length")
            .select(
                lit(batch_id).alias("batch_id"),
                col("username"),
                col("avg(length)").alias("avg_length")
            )
    )
    avg_length_per_user.write.mode("append").jdbc(JDBC_URL, "avg_toot_length_by_user", properties=JDBC_PROPS)

query = (
    filtered_df.writeStream
      .foreachBatch(sink)
      .option("checkpointLocation", "/tmp/chkpt_masto")
      .start()
)

query.awaitTermination()
