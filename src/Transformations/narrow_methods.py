import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder \
    .appName("Narrow Transformations Demo") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# ------------------------------------------
# 1️⃣ map() transformation
# ------------------------------------------
rdd = sc.parallelize([1, 2, 3, 4])

mapped_rdd = rdd.map(lambda x: x * 2)
print("map() output:", mapped_rdd.collect())

# Output:
# map() output: [2, 4, 6, 8]


# ------------------------------------------
# 2️⃣ filter() transformation
# ------------------------------------------
filtered_rdd = rdd.filter(lambda x: x > 2)
print("filter() output:", filtered_rdd.collect())

# Output:
# filter() output: [3, 4]


# ------------------------------------------
# 3️⃣ flatMap() transformation
# ------------------------------------------
text_rdd = sc.parallelize(["hello world"])

flat_rdd = text_rdd.flatMap(lambda x: x.split(" "))
print("flatMap() output:", flat_rdd.collect())

# Output:
# flatMap() output: ['hello', 'world']


# ------------------------------------------
# 4️⃣ mapPartitions()
# ------------------------------------------
partition_rdd = rdd.mapPartitions(lambda x: [sum(x)])
print("mapPartitions() output:", partition_rdd.collect())

# Output:
# mapPartitions() output: [10]


# ------------------------------------------
# 5️⃣ union()
# ------------------------------------------
rdd1 = sc.parallelize([1, 2])
rdd2 = sc.parallelize([3, 4])

union_rdd = rdd1.union(rdd2)
print("union() output:", union_rdd.collect())

# Output:
# union() output: [1, 2, 3, 4]


# ------------------------------------------
# 6️⃣ sample()
# ------------------------------------------
sample_rdd = rdd.sample(False, 0.5)
print("sample() output:", sample_rdd.collect())

# Output (random each run):
# sample() output: [1, 3]   OR [2, 4]


# ------------------------------------------
# DataFrame creation
# ------------------------------------------
data = [
    ("Alice", 21),
    ("Bob", 22),
    ("David", 23)
]

df = spark.createDataFrame(data, ["name", "Age"])


# ------------------------------------------
# 7️⃣ select()
# ------------------------------------------
df_select = df.select("name")
df_select.show()

# Output:
# +-----+
# | name|
# +-----+
# |Alice|
# |  Bob|
# |David|
# +-----+


# ------------------------------------------
# 8️⃣ withColumn()
# ------------------------------------------
df_newcol = df.withColumn("AgePlusOne", col("Age") + 1)
df_newcol.show()

# Output:
# +-----+---+-----------+
# | name|Age|AgePlusOne |
# +-----+---+-----------+
# |Alice| 21| 22        |
# | Bob | 22| 23        |
# |David| 23| 24        |
# +-----+---+-----------+


# ------------------------------------------
# 9️⃣ drop()
# ------------------------------------------
df_drop = df.drop("Age")
df_drop.show()

# Output:
# +-----+
# | name|
# +-----+
# |Alice|
# | Bob |
# |David|
# +-----+


# ------------------------------------------
# 🔟 filter() (DataFrame)
# ------------------------------------------
df_filter = df.filter(col("Age") > 21)
df_filter.show()

# Output:
# +-----+---+
# | name|Age|
# +-----+---+
# | Bob |22 |
# |David|23 |
# +-----+---+


# Stop spark
spark.stop()

