import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Spark Actions Demo") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# -------------------------------------------------
# Create RDD
# -------------------------------------------------
rdd = sc.parallelize([1, 2, 3, 4, 5])

# -------------------------------------------------
# 1️⃣ collect()
# -------------------------------------------------
print("collect():", rdd.collect())

# Output:
# collect(): [1, 2, 3, 4, 5]


# -------------------------------------------------
# 2️⃣ count()
# -------------------------------------------------
print("count():", rdd.count())

# Output:
# count(): 5


# -------------------------------------------------
# 3️⃣ take(n)
# -------------------------------------------------
print("take(3):", rdd.take(3))

# Output:
# take(3): [1, 2, 3]


# -------------------------------------------------
# 4️⃣ first()
# -------------------------------------------------
print("first():", rdd.first())

# Output:
# first(): 1


# -------------------------------------------------
# 5️⃣ reduce()
# -------------------------------------------------
sum_value = rdd.reduce(lambda x, y: x + y)
print("reduce():", sum_value)

# Output:
# reduce(): 15


# -------------------------------------------------
# 6️⃣ foreach()
# -------------------------------------------------
print("foreach() output:")
rdd.foreach(lambda x: print(x))

# Output (printed from executors):
# 1
# 2
# 3
# 4
# 5


# -------------------------------------------------
# 7️⃣ saveAsTextFile()
# -------------------------------------------------
rdd.saveAsTextFile("output_folder")

# Output:
# Folder created → output_folder/
# Contains part files with data


# -------------------------------------------------
# Create DataFrame
# -------------------------------------------------
data = [
    ("Alice", 21),
    ("Bob", 22),
    ("David", 23)
]

df = spark.createDataFrame(data, ["name", "Age"])

# -------------------------------------------------
# 8️⃣ show()
# -------------------------------------------------
df.show()

# Output:
# +-----+---+
# | name|Age|
# +-----+---+
# |Alice|21 |
# | Bob |22 |
# |David|23 |
# +-----+---+


# -------------------------------------------------
# 9️⃣ count() on DataFrame
# -------------------------------------------------
print("DataFrame count():", df.count())

# Output:
# DataFrame count(): 3


# -------------------------------------------------
# 🔟 collect() on DataFrame
# -------------------------------------------------
print("DataFrame collect():", df.collect())

# Output:
# [Row(name='Alice', Age=21),
#  Row(name='Bob', Age=22),
#  Row(name='David', Age=23)]


# -------------------------------------------------
# 1️⃣1️⃣ head()
# -------------------------------------------------
print("head():", df.head())

# Output:
# Row(name='Alice', Age=21)


# -------------------------------------------------
# 1️⃣2️⃣ write()
# -------------------------------------------------
df.write.csv("df_output")

# Output:
# Folder created → df_output/
# Contains CSV part files


# Keep Spark running for Spark UI
input("Press Enter to stop Spark...")