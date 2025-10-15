# Project 5SPAR — Mastodon Data Pipeline

End-to-end pipeline that ingests Mastodon toots in real time, processes them with Apache Spark, stores curated datasets in PostgreSQL, and surfaces insights through notebooks and sentiment analysis.

## Features
- Real-time ingestion from the Mastodon API into Kafka with optional keyword and language filters.
- Spark Structured Streaming job that cleans events and persists raw toots plus streaming aggregates.
- Batch Spark jobs for historical backfill, cleaning, and analytical aggregations.
- Lightweight sentiment analysis model built with Spark MLlib.
- Reusable Jupyter notebooks for visual exploration and demonstrations.

## Architecture
- `docker-compose.yml`: single Kafka broker exposed on `localhost:29092`.
- `docker-compose.postgres.yml`: PostgreSQL instance (with Adminer) used for raw, curated, and analytical tables.
- `docker-compose.spark.yml`: Spark + Jupyter container that mounts `./src` into `/home/jovyan/work/src`.
- `src/mastodon_to_kafka.py`: Mastodon streaming client publishing JSON payloads to Kafka.
- `src/spark_stream.py`: Structured Streaming job writing to `mastodon_posts`, `streamed_toot_counts`, and `avg_toot_length_by_user`.
- `src/batch_load_raw_fix.py`, `src/batch_clean_historical.py`, `src/batch_analytics.py`: batch processing steps for replay, cleanup, and aggregates.

## Prerequisites
- Docker Desktop or compatible runtime.
- Python 3.10+ on the host for the producer.
- Mastodon access token created via *Preferences → Development → New application*.

## Quick Start
Clone the repository and create a `.env` file at the project root:

```env
MASTODON_BASE_URL=https://mastodon.social
MASTODON_ACCESS_TOKEN=replace_me
KAFKA_BOOTSTRAP=localhost:29092
KAFKA_TOPIC=mastodon_stream
JDBC_URL=jdbc:postgresql://localhost:5433/mastodon
DB_USER=mastodon
DB_PASSWORD=mastodon
EXCLUDE_REBLOGS=true
FILTER_LANGUAGE=en
FILTER_KEYWORDS=ai,spark,data
```

Install the Python dependencies for the producer:

```bash
pip install -r requirements.txt
```

Bring up the services you need (each file can be started independently):

```bash
docker compose -f docker-compose.yml up -d              # Kafka
docker compose -f docker-compose.postgres.yml up -d    # PostgreSQL + Adminer
docker compose -f docker-compose.spark.yml up -d       # Spark + Jupyter
```

Create the Kafka topic if it does not exist:

```bash
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server localhost:9092 --create --topic mastodon_stream --partitions 3 --replication-factor 1"
```

## Streaming Pipeline
1. Run `python3 src/mastodon_to_kafka.py` to start producing toots.
2. Submit the streaming job from the Spark container:
   ```bash
   docker exec -it spark bash -lc "spark-submit /home/jovyan/work/src/spark_stream.py"
   ```
3. Inspect PostgreSQL (Adminer or `docker exec -it pg psql ...`) to confirm new rows in `mastodon_posts` and the aggregate tables.

## Batch Processing
- `src/batch_load_raw_fix.py`: replay the entire Kafka topic into `toots_raw`.
- `src/batch_clean_historical.py`: trim, normalise timestamps, and deduplicate into `toots_clean`.
- `src/batch_analytics.py`: compute hourly/daily counts, active users, and hashtag trends for reporting.

Run any batch job from inside the Spark container, for example:

```bash
docker exec -it spark bash -lc "spark-submit /home/jovyan/work/src/batch_analytics.py"
```

## Sentiment & Notebooks
- `PART4&5.ipynb`: trains a simple logistic regression sentiment model with RegexTokenizer → StopWordsRemover → TF-IDF.
- `Demo.ipynb`, `PART3.ipynb`: visualisations covering streaming/batch comparisons, hashtag trends, and sentiment distributions.

Launch Jupyter when the Spark stack is running:

```bash
docker exec -it spark bash -lc "jupyter notebook --ip=0.0.0.0 --NotebookApp.token=''"
```

Open http://localhost:8889 in your browser and run the notebooks directly against PostgreSQL using `pandas.read_sql`.

## Roadmap
- Expand the sentiment training corpus with publicly available datasets (e.g. Sentiment140).
- Consolidate Docker services into a single compose file and orchestrate jobs with Airflow or cron.
- Add automated Spark tests to validate schema expectations and transformations.
