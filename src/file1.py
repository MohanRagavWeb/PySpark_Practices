import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("First PySpark Program") \
    .getOrCreate()

# Create sample data
data = [("Mohan", 23), ("Rahul", 25), ("Priya", 22)]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

# Show Data
df.show()

spark.stop()