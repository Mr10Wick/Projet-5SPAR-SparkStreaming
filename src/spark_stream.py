# src/spark_stream.py
import os
import re

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, length, lit, to_timestamp, current_timestamp, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, LongType
)

# --- .env ---
load_dotenv()

# --- Config ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC           = os.getenv("KAFKA_TOPIC", "mastodon_stream")

JDBC_URL = os.getenv("JDBC_URL", "jdbc:postgresql://localhost:5433/mastodon")
JDBC_PROPS = {
    "user":     os.getenv("DB_USER", "mastodon"),
    "password": os.getenv("DB_PASSWORD", "mastodon"),
    "driver":   "org.postgresql.Driver",
}

# Filtres optionnels
FILTER_LANGUAGE = (os.getenv("FILTER_LANGUAGE") or "").strip().lower() or None
FILTER_KEYWORDS = [
    kw.strip().lower()
    for kw in (os.getenv("FILTER_KEYWORDS") or "").split(",")
    if kw.strip()
]

# --- Schéma d’un toot Mastodon (côté producer) ---
schema = StructType([
    StructField("id",         StringType(), True),
    StructField("created_at", StringType(), True),  # ex: "2025-10-10 17:42:33.123456+00:00"
    StructField("language",   StringType(), True),
    StructField("text",       StringType(), True),
    StructField("hashtags",   ArrayType(StringType()), True),
    StructField("user_id",    StringType(), True),
    StructField("username",   StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("favourites", LongType(), True),
    StructField("reblogs",    LongType(), True),
    StructField("replies",    LongType(), True),
    StructField("url",        StringType(), True),
])

# --- Spark ---
spark = (
    SparkSession.builder
        .appName("MastodonStreamProcessing")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3"
        )
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- Lecture depuis Kafka ---
raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("subscribe", TOPIC)
         .option("startingOffsets", "latest")  # en réel: latest
         .load()
)

df = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .select(from_json(col("json"), schema).alias("data"))
       .select("data.*")
)

# --- Normalisation timestamp (créé par Mastodon) ---
# Formats fréquents: "2025-10-10 17:42:33.123456+00:00" ou ISO8601 "2025-10-10T17:42:33.123Z"
df = df.withColumn(
    "created_at",
    coalesce(
        to_timestamp("created_at"),
        to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss.SSSSSSXXX"),
        to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
        to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        current_timestamp()
    )
)

# --- Filtres optionnels ---
filtered = df
if FILTER_LANGUAGE:
    filtered = filtered.filter(col("language") == FILTER_LANGUAGE)

if FILTER_KEYWORDS:
    # match dans le texte OU dans la liste de hashtags
    kw_regex = "(?i)(" + "|".join(re.escape(k) for k in FILTER_KEYWORDS) + ")"
    filtered = filtered.filter(
        (col("text").isNotNull() & col("text").rlike(kw_regex)) |
        (col("hashtags").isNotNull() & (col("hashtags").cast("string").rlike(kw_regex)))
    )

# --- Sink foreachBatch ---
def sink(batch_df, batch_id: int):
    # On garde les lignes valides pour écriture
    base = batch_df.filter(col("text").isNotNull() & col("username").isNotNull())

    # (0) Posts bruts -> mastodon_posts (username, content, ts)
    raw_posts = base.select(
        col("username"),
        col("text").alias("content"),
        col("created_at").alias("ts")
    )
    raw_posts.write.mode("append").jdbc(JDBC_URL, "mastodon_posts", properties=JDBC_PROPS)

    # (1) Toots par minute -> streamed_toot_counts
    per_min = (
        base.filter(col("created_at").isNotNull())
            .groupBy(window(col("created_at"), "1 minute"))
            .count()
            .select(
                lit(batch_id).alias("batch_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("count").alias("cnt")
            )
    )
    per_min.write.mode("append").jdbc(JDBC_URL, "streamed_toot_counts", properties=JDBC_PROPS)

    # (2) Longueur moyenne par user -> avg_toot_length_by_user
    avg_len = (
        base.withColumn("length", length(col("text")))
            .groupBy("username")
            .avg("length")
            .select(
                lit(batch_id).alias("batch_id"),
                col("username"),
                col("avg(length)").alias("avg_length")
            )
    )
    avg_len.write.mode("append").jdbc(JDBC_URL, "avg_toot_length_by_user", properties=JDBC_PROPS)

# --- Démarrage ---
query = (
    filtered.writeStream
            .foreachBatch(sink)
            .option("checkpointLocation", "/tmp/chkpt_masto")
            .start()
)

query.awaitTermination()
