import pandas as pd

data = {"Name": ["Mohan", "Priya", "Rahul"], "Age": [23, 22, 24]}
df = pd.DataFrame(data)

print(df[df["Age"] > 22])


import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Compare").getOrCreate()

data = [("Mohan", 23), ("Priya", 22), ("Rahul", 24)]
df = spark.createDataFrame(data, ["Name", "Age"])

df.filter(df.Age > 22).show()