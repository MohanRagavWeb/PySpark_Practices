from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AppendExample").getOrCreate()

# Data to append
data = [
    (4, "Kiran"),
    (5, "Anu")
]

df = spark.createDataFrame(data, ["id", "name"])

df.write \
.mode("append") \
.parquet("output/append_data")

print("Data appended successfully")

spark.stop()