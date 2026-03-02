from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SCD_Type4").getOrCreate()

current = [(1,"Mohan","Salem")]
current_df = spark.createDataFrame(current,["id","name","city"])

updates = [(1,"Mohan","Chennai")]
update_df = spark.createDataFrame(updates,["id","name","city"])

# Move old data to history
history_df = current_df

# Update current table
new_current = update_df

print("History Table")
history_df.show()

print("Current Table")
new_current.show()

spark.stop()