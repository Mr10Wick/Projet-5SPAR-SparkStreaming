from pyspark.sql import SparkSession
import time

spark = (
    SparkSession.builder
      .appName('HoldSparkUI')
      .getOrCreate()
)

# Affiche l'URL de l'UI dans les logs du driver
print('UI =', spark.sparkContext.uiWebUrl, flush=True)

# Garde le job en vie 5 minutes
time.sleep(300)
