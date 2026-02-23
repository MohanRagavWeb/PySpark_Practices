import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Partition Optimization Demo") \
    .master("local[*]") \
    .getOrCreate()

# Create large dataset
data = [(i, "Name_" + str(i)) for i in range(1, 1000000)]
df = spark.createDataFrame(data, ["id", "name"])

print("Initial Partitions:", df.rdd.getNumPartitions())

# Force repartition (shuffle happens)
df_repart = df.repartition(4)
print("After Repartition:", df_repart.rdd.getNumPartitions())

df_repart.count()   # Action to trigger execution

# Reduce partitions using coalesce
df_coalesce = df_repart.coalesce(1)
print("After Coalesce:", df_coalesce.rdd.getNumPartitions())

df_coalesce.count()  # Trigger execution

input("Press Enter to stop Spark...")