
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"



from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test Spark UI") \
    .master("local[*]") \
    .getOrCreate()

data = [("Alice", 21), ("Bob", 22)]
df = spark.createDataFrame(data, ["name", "age"])

df.show()
df.count()
# input("Press Enter to stop Spark...")