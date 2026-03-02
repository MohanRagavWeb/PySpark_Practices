# ROLLING SNAPSHOT LOAD

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_sub

spark = SparkSession.builder.appName("Rolling_Snapshot").getOrCreate()

df = spark.read.parquet("loads/full_snapshot")

# Keep last 7 days data
rolling_df = df.filter(
    df.snapshot_date >= date_sub(current_date(), 7)
)

rolling_df.write \
    .mode("overwrite") \
    .parquet("loads/full_snapshot")

rolling_df.show()

spark.stop()