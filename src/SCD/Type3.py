from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# -----------------------------------
# 1️⃣ Create Spark Session
# -----------------------------------
spark = SparkSession.builder \
    .appName("SCD_Type3") \
    .getOrCreate()

# -----------------------------------
# 2️⃣ Existing Target Table
# -----------------------------------
target_data = [
    (1, "Mohan", "CSE", None)
]

target_df = spark.createDataFrame(
    target_data,
    ["id", "name", "dept", "previous_dept"]
)

print("Initial Target Table")
target_df.show()

# Output:
# +---+------+----+--------------+
# |id |name  |dept|previous_dept |
# +---+------+----+--------------+
# |1  |Mohan |CSE |null          |
# +---+------+----+--------------+


# -----------------------------------
# 3️⃣ Incoming Source Data
# -----------------------------------
source_data = [
    (1, "Mohan", "IT"),   # department changed
    (2, "Priya", "ECE")   # new employee
]

source_df = spark.createDataFrame(
    source_data,
    ["id", "name", "dept"]
)

print("Incoming Source Data")
source_df.show()

# -----------------------------------
# 4️⃣ Join Source and Target
# -----------------------------------
joined_df = target_df.alias("t").join(
    source_df.alias("s"),
    "id",
    "full_outer"
)

# -----------------------------------
# 5️⃣ Apply SCD Type 3 Logic
# -----------------------------------
result_df = joined_df.select(
    col("id"),
    col("s.name").alias("name"),

    # Update department
    col("s.dept").alias("dept"),

    # Move old dept to previous_dept if changed
    col("t.dept").alias("previous_dept")
)

print("Final SCD Type 3 Table")
result_df.show()

# Expected Output:
# +---+------+----+--------------+
# |id |name  |dept|previous_dept |
# +---+------+----+--------------+
# |1  |Mohan |IT  |CSE           |
# |2  |Priya |ECE |null          |
# +---+------+----+--------------+

spark.stop()