from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
from delta.tables import DeltaTable

# -----------------------------------
# 1️⃣ Spark Session
# -----------------------------------
spark = SparkSession.builder \
    .appName("SCD_Type2") \
    .getOrCreate()

# -----------------------------------
# 2️⃣ Existing Target Table
# -----------------------------------
target_data = [
    (1, "Mohan", "Chennai", "2024-01-01", None, "Y")
]

target_df = spark.createDataFrame(
    target_data,
    ["id","name","city","start_date","end_date","is_current"]
)

target_path = "delta/scd_type2"

target_df.write.format("delta").mode("overwrite").save(target_path)

print("Initial Target Table")
spark.read.format("delta").load(target_path).show()

# -----------------------------------
# 3️⃣ Incoming Source Data
# -----------------------------------
source_data = [
    (1, "Mohan", "Bangalore"),
    (2, "Priya", "Delhi")
]

source_df = spark.createDataFrame(
    source_data,
    ["id","name","city"]
)

print("Incoming Source Data")
source_df.show()

# -----------------------------------
# 4️⃣ Load Delta Table
# -----------------------------------
deltaTable = DeltaTable.forPath(spark, target_path)

# -----------------------------------
# 5️⃣ Expire Old Records (WHEN MATCHED)
# -----------------------------------
deltaTable.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id AND target.is_current = 'Y'"
).whenMatchedUpdate(
    condition="target.city <> source.city",
    set={
        "end_date": "current_date()",
        "is_current": "'N'"
    }
).execute()

# -----------------------------------
# 6️⃣ Insert New Records
# -----------------------------------
new_records = source_df \
.withColumn("start_date", current_date()) \
.withColumn("end_date", lit(None)) \
.withColumn("is_current", lit("Y"))

deltaTable.alias("target").merge(
    new_records.alias("source"),
    "target.id = source.id AND target.is_current = 'Y'"
).whenNotMatchedInsertAll().execute()

# -----------------------------------
# 7️⃣ Final Result
# -----------------------------------
print("Final SCD Type 2 Table")
spark.read.format("delta").load(target_path).show()

spark.stop()