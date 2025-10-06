from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, to_date, date_trunc, length, lower, trim, explode,
                                   count as f_count, avg as f_avg, row_number, coalesce)
from pyspark.sql.window import Window
import os

JDBC_URL   = 'jdbc:postgresql://pg:5432/mastodon'
JDBC_PROPS = {'user':'mastodon','password':'mastodon','driver':'org.postgresql.Driver'}

spark = (SparkSession.builder
         .appName('BatchAnalyticsToots')
         .config('spark.sql.shuffle.partitions','4')
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

df = spark.read.jdbc(JDBC_URL, 'toots_clean', properties=JDBC_PROPS)

# timestamp non-null pour l'analytics
ts = coalesce(col('created_at'), col('ingested_at'))

df_repart = df.repartition(4).cache()
_ = df_repart.count()  # matérialise le cache

# 1) Comptes par heure/jour
hourly = df_repart.groupBy(date_trunc('hour', ts).alias('hour')).agg(f_count('*').alias('toots'))
hourly.write.mode('overwrite').jdbc(JDBC_URL,'hourly_toot_counts',properties=JDBC_PROPS)

daily = df_repart.groupBy(to_date(ts).alias('day')).agg(f_count('*').alias('toots'))
daily.write.mode('overwrite').jdbc(JDBC_URL,'daily_toot_counts',properties=JDBC_PROPS)

# 2) Activité utilisateurs
user_activity = df_repart.groupBy('username').agg(f_count('*').alias('toot_count'))
user_activity.write.mode('overwrite').jdbc(JDBC_URL,'user_activity_counts',properties=JDBC_PROPS)

active_min = int(os.environ.get('ACTIVE_MIN','5'))
user_activity.filter(col('toot_count')>=active_min)              .write.mode('overwrite').jdbc(JDBC_URL,'active_users_gtX',properties=JDBC_PROPS)

# 3) Hashtags par jour + top hashtag
hashtags = (df_repart
            .select(to_date(ts).alias('day'), explode(col('hashtags')).alias('hashtag'))
            .filter(col('hashtag').isNotNull() & (trim(col('hashtag'))!=''))
            .withColumn('hashtag', lower(trim(col('hashtag')))))

hashtags_per_day = hashtags.groupBy('day','hashtag').agg(f_count('*').alias('cnt'))
hashtags_per_day.write.mode('overwrite').jdbc(JDBC_URL,'hashtags_per_day_counts',properties=JDBC_PROPS)

w = Window.partitionBy('day').orderBy(col('cnt').desc(), col('hashtag').asc())
top_hashtag_per_day = hashtags_per_day.withColumn('rn', row_number().over(w)).filter(col('rn')==1).drop('rn')
top_hashtag_per_day.write.mode('overwrite').jdbc(JDBC_URL,'top_hashtag_per_day',properties=JDBC_PROPS)

# 4) Longueur moyenne par utilisateur
df_repart.select('username', length(trim(col('text'))).alias('len'))          .groupBy('username').agg(f_avg('len').alias('avg_len'))          .write.mode('overwrite').jdbc(JDBC_URL,'avg_toot_length_by_user_batch',properties=JDBC_PROPS)

spark.catalog.clearCache()
spark.stop()
