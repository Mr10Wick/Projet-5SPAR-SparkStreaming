Project 5SPAR - Mastodon Data Pipeline with Apache Spark

1. Overview
This project delivers an end-to-end data pipeline for Mastodon toots. It satisfies the final assignment requirements by combining real-time ingestion through Apache Kafka, streaming and batch processing with Apache Spark, persistence in PostgreSQL, a basic sentiment analysis model with Spark MLlib, and result exploration inside Jupyter notebooks.

2. Architecture and Main Components
Kafka (docker-compose.yml): single broker based on apache/kafka:3.7.0, exposed on localhost:29092 for host clients.
Mastodon producer (src/mastodon_to_kafka.py): Python script that connects to the Mastodon public API and publishes JSON payloads to Kafka.
Spark Structured Streaming (src/spark_stream.py): Spark job that consumes mastodon_stream, cleans the records, and writes them into PostgreSQL.
PostgreSQL plus Adminer (docker-compose.postgres.yml): database to store raw toots, streaming aggregates, batch analytics, and sentiment outputs.
Batch Spark jobs (src/batch_load_raw_fix.py, src/batch_clean_historical.py, src/batch_analytics.py): historical replay from Kafka, cleanup, and analytical aggregations.
Jupyter notebooks (Demo.ipynb, PART3.ipynb, PART4&5.ipynb): exploratory analysis, machine learning training, and plotting with Matplotlib and Seaborn.

3. Prerequisites and Configuration
Software: Docker Desktop (or compatible engine) to launch Kafka, Spark, and PostgreSQL, plus Python 3.10 or newer on the host machine to run the producer.
Python dependencies: for the producer run pip install -r requirements.txt (Mastodon.py, confluent-kafka, python-dotenv). For notebooks and machine learning also install pandas, matplotlib, seaborn, sqlalchemy, psycopg2-binary in the notebook environment.
Environment file (.env at repository root):
MASTODON_BASE_URL=https://mastodon.social
MASTODON_ACCESS_TOKEN=... (token generated from Mastodon application)
KAFKA_BOOTSTRAP=localhost:29092
KAFKA_TOPIC=mastodon_stream
JDBC_URL=jdbc:postgresql://localhost:5433/mastodon
DB_USER=mastodon
DB_PASSWORD=mastodon
EXCLUDE_REBLOGS=true
FILTER_LANGUAGE=en (optional)
FILTER_KEYWORDS=ai,spark,data (optional)
python-dotenv loads these variables automatically in the Python scripts.

4. Starting the Infrastructure
Kafka: docker compose -f docker-compose.yml up -d
PostgreSQL and Adminer: docker compose -f docker-compose.postgres.yml up -d
Spark and Jupyter notebook: docker compose -f docker-compose.spark.yml up -d (the spark container mounts ./src inside /home/jovyan/work/src)
Kafka topic creation if needed:
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --create --topic mastodon_stream --partitions 3 --replication-factor 1"

5. Part 1 - Real-time Ingestion with Kafka
Register a Mastodon application using the Mastodon web UI (Preferences > Development > New application) to obtain an access token.
Run the Python producer: python3 src/mastodon_to_kafka.py
The script opens the stream_public endpoint, strips HTML, extracts hashtags and metadata, applies optional filters (language, keywords, reblog exclusion), and publishes JSON messages to the mastodon_stream topic.
Quick verification:
docker exec -it kafka bash -lc "kafka-console-consumer --bootstrap-server localhost:9092 --topic mastodon_stream --from-beginning --max-messages 5"
Sample export: redirect the consumer output to sample.jsonl to build a small test dataset.

6. Part 2 - Streaming with Spark Structured Streaming
Submit the job from the Spark container:
docker exec -it spark bash -lc "spark-submit /home/jovyan/work/src/spark_stream.py"
Highlights of src/spark_stream.py:
Reads from Kafka via the host-exposed port 29092.
Parses JSON with a strict Mastodon schema (id, text, hashtags, account, engagement metrics).
Normalises timestamps with a coalesce of multiple patterns.
Applies dynamic filters driven by the .env file.
Writes to PostgreSQL using foreachBatch in three tables: mastodon_posts (raw toot with username, content, timestamp), streamed_toot_counts (rolling one-minute aggregates: window_start, window_end, cnt, batch_id), avg_toot_length_by_user (average length per user per micro-batch).
Uses a checkpoint at /tmp/chkpt_masto to support restarts.

7. Part 3 - Batch Processing on Historical Data
Backfill Kafka to PostgreSQL raw (src/batch_load_raw_fix.py): replays the entire Kafka topic in batch mode, trims text, parses timestamps, and appends to toots_raw.
Cleaning and deduplication (src/batch_clean_historical.py): trims string columns, normalises several timestamp formats, removes duplicates using row_number, and writes the cleaned dataset to toots_clean.
Analytical aggregations (src/batch_analytics.py): caches the working DataFrame (repartition(4).cache()), computes and overwrites hourly_toot_counts, daily_toot_counts, user_activity_counts, active_users_gtX (threshold ACTIVE_MIN, default 5 toots), hashtags_per_day_counts, top_hashtag_per_day, avg_toot_length_by_user_batch. These outputs power the dashboards and allow comparisons between streaming and batch results.

8. Part 4 - Sentiment Analysis with Spark MLlib
Implemented in PART4&5.ipynb:
Pre-processing loads cleaned toots from mastodon_posts, converts text to lowercase, and removes punctuation with regex.
Labelling strategy combines a tiny hand-crafted positive and negative corpus with regex-based auto labelling using keyword lexicons to enlarge the training set.
MLlib pipeline: RegexTokenizer -> StopWordsRemover -> CountVectorizer -> IDF -> LogisticRegression.
Inference applies the fitted model to historical toots and stores results in mastodon_sentiments (text, sentiment integer).
Limitations: English-only keywords, small training volume, and qualitative evaluation. Next step is to incorporate an external dataset (for example Kaggle Sentiment140) for better accuracy.

9. Part 5 - Visualisations and Demonstration
The notebooks rely on pandas, sqlalchemy, matplotlib, and seaborn to generate sentiment distribution charts (mastodon_sentiments), time series (toots per minute, 15-minute rolling average, toots per day), user activity heatmaps, top hashtags extracted from arrays or regex, and supporting figures for the live demonstration.
Launch Jupyter:
docker exec -it spark bash -lc "jupyter notebook --ip=0.0.0.0 --NotebookApp.token=''"
Access the UI at http://localhost:8889
Useful SQL snippets:
docker exec -it pg psql -U mastodon -d mastodon -c "SELECT COUNT(*) FROM mastodon_posts;"
docker exec -it pg psql -U mastodon -d mastodon -c "SELECT * FROM streamed_toot_counts ORDER BY window_start DESC LIMIT 10;"
docker exec -it pg psql -U mastodon -d mastodon -c "SELECT sentiment, COUNT(*) FROM mastodon_sentiments GROUP BY 1;"
Suggested demo flow:
Start Kafka, PostgreSQL, and Spark.
Run mastodon_to_kafka.py.
Submit spark_stream.py and display inserts in PostgreSQL (Adminer or psql).
Refresh the notebook cells (pandas.read_sql) to show new toots arriving live, updated streaming aggregates, and the sentiment distribution.

10. Limitations and Next Steps
Expand the sentiment model with a larger labelled corpus (for instance Kaggle Sentiment140).
Add a Kafka UI such as provectuslabs/kafka-ui for topic inspection if required.
Merge services into a single docker-compose file and schedule batch jobs via Airflow or Spark cron.
Introduce automated tests (for example PySpark assertDataFrameEqual) to validate pipeline steps.
