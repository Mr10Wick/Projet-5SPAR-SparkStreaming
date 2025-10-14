# Projet 5SPAR - Spark Streaming Mastodon

#### 1. Kafka Stream Consumption

I configured Spark to connect to Kafka through the topic `mastodon_stream`. Each message in Kafka is a JSON object representing a Mastodon toot.  
These messages are read in streaming mode with Spark Structured Streaming.

The raw messages are then parsed using a predefined schema into structured DataFrames. This schema includes fields such as `id`, `created_at`, `language`, `text`, `username`, `hashtags`, etc.  
The `created_at` field was converted into a proper timestamp with `to_timestamp`.

---

#### 2. Transformations

Two key transformations were implemented on the incoming data.  
First, I applied filters to only keep relevant messages based on a chosen language or specific keywords (for example: "spark", "apache", "données").  
Then, I applied a **windowing operation**: toots were grouped into **one-minute windows** to allow time-based aggregations.

---

#### 3. Actions and Sink

The core of the processing logic is inside the `sink` function.  
This function defines what to do for each micro-batch of data processed by Spark:

- **Action 1: Count of toots per one-minute window**  
  For each micro-batch, the number of toots per time window is calculated and stored in PostgreSQL in the table `streamed_toot_counts`.

- **Action 2: Average toot length per user**  
  For each user, the average length of their toots in the batch is calculated and stored in PostgreSQL in the table `avg_toot_length_by_user`.

These two actions ensure both time-based insights (toot frequency) and user-based insights (average message size).

---

#### 4. Data Persistence in PostgreSQL

The results from the sink are continuously written into PostgreSQL via JDBC.  
I verified the persistence by connecting to PostgreSQL and running queries to check the latest rows from the two tables:
- `streamed_toot_counts`
- `avg_toot_length_by_user`

This confirmed that the streaming pipeline works end-to-end: data flows from Kafka → Spark → PostgreSQL.

---

#### 5. Quick Run Commands

To reproduce and run the pipeline:

```bash
# Start all services (Spark, Kafka, PostgreSQL, Zookeeper if needed)
docker-compose up -d

# Run the Spark job inside the container
docker exec -it spark bash -lc "spark-submit /home/jovyan/work/spark_stream.py"

# Test Kafka topic creation and messages
docker exec -it 5spar-kafka bash -lc "kafka-topics --bootstrap-server localhost:19092 --describe --topic mastodon_stream"

# Send sample data into Kafka
docker exec -i 5spar-kafka bash -lc "kafka-console-producer --bootstrap-server localhost:19092 --topic mastodon_stream" < sample.jsonl

# Consume a few messages from Kafka to verify ingestion
docker exec -it 5spar-kafka bash -lc "kafka-console-consumer --bootstrap-server localhost:19092 --topic mastodon_stream --from-beginning --max-messages 5"

# Query PostgreSQL for results
docker exec -it pg bash -lc "psql -U mastodon -d mastodon -c 'SELECT * FROM streamed_toot_counts ORDER BY processed_at DESC LIMIT 10;'"
docker exec -it pg bash -lc "psql -U mastodon -d mastodon -c 'SELECT * FROM avg_toot_length_by_user ORDER BY processed_at DESC LIMIT 10;'"
