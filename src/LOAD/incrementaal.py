# INCREMENTAL LOAD PROGRAM

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Incremental_Load").getOrCreate()

path = "loads/incremental_load"

# Initial Target Data
target = [(1,"Mohan","Chennai"), (2,"Ravi","Delhi")]
target_df = spark.createDataFrame(target,["id","name","city"])

target_df.write.format("delta").mode("overwrite").save(path)

# Incremental Source Data
source = [(2,"Ravi","Mumbai"), (3,"Arun","Bangalore")]
source_df = spark.createDataFrame(source,["id","name","city"])

# Merge (Upsert)
deltaTable = DeltaTable.forPath(spark, path)

deltaTable.alias("t").merge(
    source_df.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

spark.read.format("delta").load(path).show()

spark.stop()