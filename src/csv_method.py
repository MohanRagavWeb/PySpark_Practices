import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


spark= SparkSession.builder.appName("csv_method").getOrCreate()

df=spark.read.csv("students.csv",header=True,inferSchema=True)
# df.select("name").show()
# df.filter(df.age>21).show()
# df.filter(df.name == "David").show()
# df.withColumn("Country", lit("India")).show()
# df.withColumn("CopyAge", df.age).show()

df.groupBy("age").count().show()
# input("Press Enter to stop.....")