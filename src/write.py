from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WriteFiles").getOrCreate()

data = [
    ("Mohan", 23, 5000),
    ("Priya", 22, 6000)
]

df = spark.createDataFrame(data, ["Name", "Age", "Salary"])

# 1️⃣ Write CSV
df.write.mode("overwrite").csv("output/csv_data", header=True)

# 2️⃣ Write Parquet
df.write.mode("overwrite").parquet("output/parquet_data")

# 3️⃣ Write JSON
df.write.mode("overwrite").json("output/json_data")

# 4️⃣ Write Delta
df.write.format("delta").mode("overwrite").save("output/delta_data")

spark.stop()