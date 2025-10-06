import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
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
      .appName("BatchBackfillRawToots")
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

df = df.withColumn(
    "created_at",
    to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
)

(df.write
   .mode("append")
   .jdbc(JDBC_URL, "toots_raw", properties=JDBC_PROPS))

spark.stop()
