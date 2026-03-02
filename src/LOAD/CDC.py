# CDC PROGRAM

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("CDC").getOrCreate()

old_data = [(1,"Mohan","Chennai"), (2,"Ravi","Delhi")]
new_data = [(2,"Ravi","Mumbai"), (3,"Arun","Bangalore")]

old_df = spark.createDataFrame(old_data,["id","name","city"])
new_df = spark.createDataFrame(new_data,["id","name","city"])

# Inserts
inserts = new_df.join(old_df,"id","left_anti") \
    .withColumn("operation",lit("INSERT"))

# Deletes
deletes = old_df.join(new_df,"id","left_anti") \
    .withColumn("operation",lit("DELETE"))

# Updates
updates = new_df.alias("n").join(
    old_df.alias("o"),"id"
).filter("n.city != o.city") \
 .select("n.*") \
 .withColumn("operation",lit("UPDATE"))

cdc_df = inserts.union(updates).union(deletes)

cdc_df.show()

spark.stop()