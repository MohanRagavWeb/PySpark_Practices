from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("SCD_Type6").getOrCreate()

# Old record
target = [(1,"Mohan","Salem",None,"Y")]
target_df = spark.createDataFrame(
    target,
    ["id","name","city","prev_city","active"]
)

# New update
source = [(1,"Mohan","Chennai")]
source_df = spark.createDataFrame(source,["id","name","city"])

# Close old record (Type 2)
old_closed = target_df.withColumn("active",lit("N"))

# Insert new version
new_version = source_df \
    .withColumn("prev_city",lit("Salem")) \
    .withColumn("active",lit("Y"))

final_df = old_closed.unionByName(new_version)

final_df.show()

spark.stop()