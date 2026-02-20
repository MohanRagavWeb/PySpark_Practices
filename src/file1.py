import os

# ensure PySpark uses the current virtual environment python
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Practice") \
    .getOrCreate()

# get spark context
sc = spark.sparkContext

# create RDD
rdd = sc.parallelize([1, 2, 3, 4, 5], 2)

# output
print("Partitions:", rdd.getNumPartitions())
print("Data:", rdd.collect())