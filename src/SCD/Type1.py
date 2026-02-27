from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# -----------------------------------
# 1️⃣ Create Spark Session
# -----------------------------------
spark = SparkSession.builder \
    .appName("SCD_Type1") \
    .getOrCreate()

# -----------------------------------
# 2️⃣ Existing Target Table (Old Data)
# -----------------------------------
target_data = [
    (1, "Mohan", "Chennai"),
    (2, "Priya", "Delhi")
]

target_df = spark.createDataFrame(
    target_data,
    ["id", "name", "city"]
)

# Save as Delta table
target_path = "delta/scd_type1"

target_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(target_path)

print("Initial Target Table:")
spark.read.format("delta").load(target_path).show()

# Output:
# +---+------+-------+
# |id |name  |city   |
# +---+------+-------+
# |1  |Mohan |Chennai|
# |2  |Priya |Delhi  |
# +---+------+-------+


# -----------------------------------
# 3️⃣ Incoming Source Data (Updated)
# -----------------------------------
source_data = [
    (2, "Priya", "Mumbai"),   # city changed
    (3, "Rahul", "Bangalore") # new record
]

source_df = spark.createDataFrame(
    source_data,
    ["id", "name", "city"]
)

print("Incoming Source Data:")
source_df.show()

# -----------------------------------
# 4️⃣ Load Delta Table
# -----------------------------------
deltaTable = DeltaTable.forPath(spark, target_path)

# -----------------------------------
# 5️⃣ SCD TYPE 1 MERGE (Overwrite Update)
# -----------------------------------
deltaTable.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(set={
    "name": "source.name",
    "city": "source.city"
}).whenNotMatchedInsert(values={
    "id": "source.id",
    "name": "source.name",
    "city": "source.city"
}).execute()

# -----------------------------------
# 6️⃣ Final Table After Type 1 Update
# -----------------------------------
print("Final Table After SCD Type 1:")
spark.read.format("delta").load(target_path).show()

# Expected Output:
# +---+------+----------+
# |id |name  |city      |
# +---+------+----------+
# |1  |Mohan |Chennai   |
# |2  |Priya |Mumbai    |  <-- updated (overwrite)
# |3  |Rahul |Bangalore |  <-- inserted
# +---+------+----------+

spark.stop()