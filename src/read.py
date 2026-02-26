from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadFiles").getOrCreate()

# Schema
schema = StructType([
    StructField("Name", StringType()),
    StructField("Age", IntegerType()),
    StructField("Salary", DoubleType())
])

# CSV
csv_df = spark.read.schema(schema).csv("data/file.csv", header=True)
csv_df.show()

# JSON
json_df = spark.read.schema(schema).json("data/file.json")
json_df.show()

# Parquet
parquet_df = spark.read.parquet("data/file.parquet")
parquet_df.show()

spark.stop()