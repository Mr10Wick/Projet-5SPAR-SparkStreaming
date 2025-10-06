from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace, to_timestamp, coalesce, row_number
from pyspark.sql.window import Window

JDBC_URL   = 'jdbc:postgresql://pg:5432/mastodon'
JDBC_PROPS = {'user':'mastodon','password':'mastodon','driver':'org.postgresql.Driver'}

spark = (SparkSession.builder
         .appName('BatchCleanHistoricalToots')
         .config('spark.sql.shuffle.partitions','4')
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

raw = spark.read.jdbc(JDBC_URL, 'toots_raw', properties=JDBC_PROPS)

# Trim champs texte
df = (raw
      .withColumn('text', trim(col('text')))
      .withColumn('username', trim(col('username'))))

# Normalisation timestamps (plusieurs patterns)
ts_s = col('created_at').cast('string')
ts_s_norm = regexp_replace(ts_s, r'Z$', '+00:00')  # remplace Z par +00:00

cand1 = to_timestamp(ts_s_norm, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
cand2 = to_timestamp(ts_s_norm, "yyyy-MM-dd'T'HH:mm:ssXXX")
cand3 = to_timestamp(ts_s,       "yyyy-MM-dd HH:mm:ssXXX")
cand4 = to_timestamp(ts_s,       "yyyy-MM-dd HH:mm:ss")

created_fixed = coalesce(col('created_at').cast('timestamp'), cand1, cand2, cand3, cand4)
df = df.withColumn('created_at', created_fixed)

# Déduplication: garder la ligne la plus récente par id
w = Window.partitionBy('id').orderBy(col('created_at').desc_nulls_last())
dedup = (df.withColumn('rn', row_number().over(w))
           .filter(col('rn')==1)
           .drop('rn'))

dedup.write.mode('overwrite').jdbc(JDBC_URL, 'toots_clean', properties=JDBC_PROPS)

spark.stop()
