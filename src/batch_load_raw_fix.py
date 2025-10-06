import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, coalesce, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "mastodon_stream"

JDBC_URL = "jdbc:postgresql://pg:5432/mastodon"
JDBC_PROPS = {"user": "mastodon", "password": "mastodon", "driver": "org.postgresql.Driver"}

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
      .appName("BatchBackfillRawTootsFix")
      .config("spark.jars.packages",
              "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3")
      .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
      .option("subscribe", TOPIC)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
)

df = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .select(from_json(col("json"), schema).alias("d"))
       .select("d.*")
)

# Nettoyage basique
df = df.withColumn("text", trim(col("text"))).withColumn("username", trim(col("username")))
df = df.filter(col("id").isNotNull() & col("text").isNotNull() & (trim(col("text")) != ""))

# Parsing robuste du timestamp
# - Normalise un Z final -> +00:00
# - Essaie plusieurs patterns courants
ts = col("created_at")
ts_norm = regexp_replace(ts, "Z$", "+00:00")

df = df.withColumn(
    "created_at",
    coalesce(
        to_timestamp(ts_norm, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
        to_timestamp(ts_norm, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        to_timestamp(ts_norm, "yyyy-MM-dd HH:mm:ss.SSSSSSXXX"),
        to_timestamp(ts_norm, "yyyy-MM-dd HH:mm:ssXXX"),
        to_timestamp(ts),  # dernier recours: auto-parsing de Spark
    )
)

# Ecrit dans toots_raw (append)
(df.write
   .mode("append")
   .jdbc(JDBC_URL, "toots_raw", properties=JDBC_PROPS))

spark.stop()
