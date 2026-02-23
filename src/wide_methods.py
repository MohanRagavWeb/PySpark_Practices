
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"




from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("Wide Transformations Demo") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# ---------------------------------------------------
# 1️⃣ reduceByKey()  (RDD wide transformation)
# ---------------------------------------------------
rdd = sc.parallelize([("A",1), ("A",2), ("B",3), ("B",4)])

reduce_rdd = rdd.reduceByKey(lambda x,y: x+y)
print("reduceByKey output:", reduce_rdd.collect())

# Output:
# reduceByKey output: [('A', 3), ('B', 7)]

# Spark UI:
# → Shuffle Read & Shuffle Write visible
# → New stage created


# ---------------------------------------------------
# 2️⃣ DataFrame for further operations
# ---------------------------------------------------
data = [
    ("Alice", 21),
    ("Bob", 22),
    ("David", 21),
    ("Eva", 23),
    ("John", 22)
]

df = spark.createDataFrame(data, ["name", "Age"])


# ---------------------------------------------------
# 3️⃣ groupBy()
# ---------------------------------------------------
group_df = df.groupBy("Age").count()
group_df.show()

# Output:
# +---+-----+
# |Age|count|
# +---+-----+
# |21 |  2  |
# |22 |  2  |
# |23 |  1  |
# +---+-----+

# Spark UI:
# → Shuffle occurs
# → Stage split
# → Multiple tasks created


# ---------------------------------------------------
# 4️⃣ join()
# ---------------------------------------------------
dept_data = [
    ("Alice", "IT"),
    ("Bob", "HR"),
    ("David", "Finance")
]

dept_df = spark.createDataFrame(dept_data, ["name", "dept"])

join_df = df.join(dept_df, "name")
join_df.show()

# Output:
# +-----+---+--------+
# | name|Age|   dept |
# +-----+---+--------+
# |Alice|21 | IT     |
# | Bob |22 | HR     |
# |David|21 |Finance |
# +-----+---+--------+

# Spark UI:
# → Big shuffle stage
# → Shuffle read/write
# → Join execution visible


# ---------------------------------------------------
# 5️⃣ orderBy()
# ---------------------------------------------------
order_df = df.orderBy("Age")
order_df.show()

# Output:
# +-----+---+
# | name|Age|
# +-----+---+
# |Alice|21 |
# |David|21 |
# | Bob |22 |
# |John |22 |
# | Eva |23 |
# +-----+---+

# Spark UI:
# → Full shuffle
# → Sorting stage created


# ---------------------------------------------------
# 6️⃣ distinct()
# ---------------------------------------------------
dup_data = [
    ("Alice",21),
    ("Alice",21),
    ("Bob",22)
]

dup_df = spark.createDataFrame(dup_data, ["name","Age"])

distinct_df = dup_df.distinct()
distinct_df.show()

# Output:
# +-----+---+
# | name|Age|
# +-----+---+
# |Alice|21 |
# | Bob |22 |
# +-----+---+

# Spark UI:
# → Shuffle to remove duplicates


# ---------------------------------------------------
# 7️⃣ repartition()
# ---------------------------------------------------
repart_df = df.repartition(4)
print("Partitions after repartition:", repart_df.rdd.getNumPartitions())

# Output:
# Partitions after repartition: 4

# Spark UI:
# → Shuffle stage created
# → Data redistributed across executors


# Keep program running for Spark UI observation
input("Press Enter to stop Spark...")