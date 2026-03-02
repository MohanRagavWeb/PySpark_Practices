# FULL LOAD PROGRAM

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Full_Load").getOrCreate()

# Source Data
data = [
    (1, "Mohan", "Chennai"),
    (2, "Priya", "Delhi")
]

source_df = spark.createDataFrame(data, ["id", "name", "city"])

# Full Load (Overwrite)
source_df.write \
    .mode("overwrite") \
    .parquet("loads/full_load")

print("Full Load Completed")

spark.stop()