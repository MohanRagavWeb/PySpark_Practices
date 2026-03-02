from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SCD_Type5").getOrCreate()

current = [(1,"Mohan","Salem")]
current_df = spark.createDataFrame(current,["id","name","city"])

update = [(1,"Mohan","Chennai")]
update_df = spark.createDataFrame(update,["id","name","city"])

# Save old record to history
history_df = current_df

# Overwrite current (Type 1)
current_df = update_df

print("Current Table")
current_df.show()

print("History Table")
history_df.show()

spark.stop()