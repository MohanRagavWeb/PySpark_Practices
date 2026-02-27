from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder \
.appName("UpsertExample") \
.getOrCreate()

path = "output/delta_upsert"

# -------------------
# Target Data
# -------------------
target_data = [
    (1, "Mohan", "Chennai"),
    (2, "Priya", "Delhi")
]

target_df = spark.createDataFrame(
    target_data, ["id", "name", "city"]
)

target_df.write.format("delta").mode("overwrite").save(path)

# -------------------
# Source Data
# -------------------
source_data = [
    (2, "Priya", "Mumbai"),   # update
    (3, "Rahul", "Bangalore") # insert
]

source_df = spark.createDataFrame(
    source_data, ["id", "name", "city"]
)

# -------------------
# MERGE (UPSERT)
# -------------------
deltaTable = DeltaTable.forPath(spark, path)

deltaTable.alias("t").merge(
    source_df.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

spark.read.format("delta").load(path).show()

spark.stop()