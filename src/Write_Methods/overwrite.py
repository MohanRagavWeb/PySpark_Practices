from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("OverwriteExample").getOrCreate()

# New Data
data = [
    (2, "Rahul"),
    (3, "Priya")
]

df = spark.createDataFrame(data, ["id", "name"])

# OVERWRITE WRITE
df.write \
.mode("overwrite") \
.csv("output/overwrite_data", header=True)

print("Data written using OVERWRITE mode")

spark.stop()