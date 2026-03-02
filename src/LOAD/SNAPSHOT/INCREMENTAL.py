# INCREMENTAL SNAPSHOT LOAD

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

spark = SparkSession.builder.appName("Incremental_Snapshot").getOrCreate()

# Only changed records
changes = [(2,"Priya",65000)]

df = spark.createDataFrame(changes,["id","name","salary"])

inc_snapshot = df.withColumn("snapshot_date", current_date())

inc_snapshot.write \
    .mode("append") \
    .parquet("loads/incremental_snapshot")

spark.read.parquet("loads/incremental_snapshot").show()

spark.stop()