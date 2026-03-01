import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"



from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PartitionOverwrite").getOrCreate()

# Enable dynamic partition overwrite
spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)

data = [
    ("Rahul", "India"),
    ("John", "USA")
]

df = spark.createDataFrame(data, ["name", "country"])

df.write \
.mode("overwrite") \
.partitionBy("country") \
.parquet("output/partition_data")

print("Partition overwrite completed")

spark.stop()