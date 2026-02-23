import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"




from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CachingExample").getOrCreate()

data = [
    ("Mohan", 23),
    ("Priya", 22),
    ("Rahul", 24),
    ("Anu", 21)
]

df = spark.createDataFrame(data, ["Name", "Age"])

# Cache DataFrame
df.cache()

print("First operation:")
df.filter(df.Age > 22).show()

print("Second operation:")
df.groupBy("Age").count().show()

spark.stop()