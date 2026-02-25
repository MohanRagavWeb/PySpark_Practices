from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()

data = [
    (1, "Mohan", "CSE", 5000),
    (2, "Priya", "CSE", 6000),
    (3, "Rahul", "IT", 6000),
    (4, "Anu", "IT", 4000),
    (5, "Kiran", "CSE", 5000)
]

df = spark.createDataFrame(data, ["emp_id", "name", "dept", "salary"])

windowSpec = Window.partitionBy("dept").orderBy("salary")

# ROW_NUMBER
df.withColumn("row_number", row_number().over(windowSpec)).show()

# Output:
# Unique row numbers inside each department


# RANK
df.withColumn("rank", rank().over(windowSpec)).show()

# Output:
# Same salary → same rank
# gaps appear


# DENSE_RANK
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

# Output:
# Same salary → same rank
# no gaps


# LEAD
df.withColumn("next_salary", lead("salary", 1).over(windowSpec)).show()

# Output:
# Shows next row salary inside dept


# LAG
df.withColumn("prev_salary", lag("salary", 1).over(windowSpec)).show()

# Output:
# Shows previous row salary inside dept

spark.stop()