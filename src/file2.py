import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

data = [
    ("User1", 1000),
    ("User2", 50000),
    ("User3", 200)
]

df = spark.createDataFrame(data, ["User", "TransactionAmount"])

# Detect large transactions
df.filter(df.TransactionAmount > 10000).show()