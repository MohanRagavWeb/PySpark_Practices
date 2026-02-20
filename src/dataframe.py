

import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame Example").getOrCreate()

data = [("Mohan", 23), ("Priya", 21)]

df = spark.createDataFrame(data, ["Name", "Age"])

df.show()