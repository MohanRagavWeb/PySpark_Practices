from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Step 1: Create Spark Session
spark = SparkSession.builder \
    .appName("SCD_Type1") \
    .getOrCreate()

# Step 2: Existing data (Target table)
target_data = [
    (1, "Mohan", "Salem"),
    (2, "Ravi", "Chennai")
]

target_df = spark.createDataFrame(
    target_data,
    ["id", "name", "city"]
)

target_df.show()

# Step 3: Incoming data (Source table)
source_data = [
    (1, "Mohan", "Bangalore"),
    (3, "Arun", "Madurai")
]

source_df = spark.createDataFrame(
    source_data,
    ["id", "name", "city"]
)

source_df.show()

# Step 4: Apply SCD Type 1 logic
combined_df = source_df.union(target_df)

scd_type1_df = combined_df.dropDuplicates(["id"])

scd_type1_df.show()

# Step 5: Write result
scd_type1_df.write.mode("overwrite").parquet("output_scd_type1")