# FULL SNAPSHOT LOAD

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

spark = SparkSession.builder.appName("Full_Snapshot").getOrCreate()

data = [(1,"Mohan",50000),(2,"Priya",60000)]

df = spark.createDataFrame(data,["id","name","salary"])

snapshot_df = df.withColumn("snapshot_date", current_date())

snapshot_df.write \
    .mode("append") \
    .parquet("loads/full_snapshot")

spark.read.parquet("loads/full_snapshot").show()

spark.stop()