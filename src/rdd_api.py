
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"



from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD Example").getOrCreate()

sc = spark.sparkContext

data = [1,2,3,4,5]

rdd = sc.parallelize(data)

print(rdd.collect())