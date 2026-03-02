from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SCD_Type0").getOrCreate()

# Existing Data
target = [(1,"Mohan",2022)]
target_df = spark.createDataFrame(target,["id","name","join_year"])

# Incoming Data (change attempt)
source = [(1,"Mohan",2024),(2,"Priya",2023)]
source_df = spark.createDataFrame(source,["id","name","join_year"])

# Insert only new records
result = target_df.union(
    source_df.join(target_df,"id","left_anti")
)

result.show()

spark.stop()