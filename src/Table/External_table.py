import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"



from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ExternalTable").getOrCreate()

data = [
    (1, "Rahul"),
    (2, "Anu")
]

df = spark.createDataFrame(data, ["id", "name"])

# Create External Table
df.write \
.mode("overwrite") \
.option("path", "output/external_table") \
.saveAsTable("customers_ext")

spark.sql("SELECT * FROM customers_ext").show()

spark.stop()