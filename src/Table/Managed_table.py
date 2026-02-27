from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ManagedTable").getOrCreate()

data = [
    (1, "Mohan"),
    (2, "Priya")
]

df = spark.createDataFrame(data, ["id", "name"])

# Create Managed Table
df.write.mode("overwrite").saveAsTable("employees")

# Query table
spark.sql("SELECT * FROM employees").show()

spark.stop()