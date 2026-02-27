import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"


from pyspark.sql import SparkSession

spark= SparkSession.builder.appName("rdd").getOrCreate()
sc=spark.sparkContext
numbers=sc.parallelize([1,2,3,4,5])

result= numbers.map(lambda x:x*x)
print("Spark UI URL:", spark.sparkContext.uiWebUrl)
print(result.collect())