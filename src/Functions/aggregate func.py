import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"



from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("AggregateFunctions").getOrCreate()

data = [
    ("Mohan", "CSE", 5000),
    ("Priya", "IT", 7000),
    ("Rahul", "CSE", 6000),
    ("Anu", "IT", 4000),
    ("Kiran", "CSE", 5000)
]

df = spark.createDataFrame(data, ["Name", "Department", "Salary"])


# mean()
df.select(mean("Salary")).show()

# Output:
# +------------+
# |avg(Salary) |
# +------------+
# |   5400.0   |
# +------------+


# avg()
df.select(avg("Salary")).show()

# Output:
# Same as mean → 5400


# collect_list()
df.groupBy("Department").agg(collect_list("Salary")).show()

# Output:
# CSE → [5000,6000,5000]
# IT  → [7000,4000]


# collect_set()
df.groupBy("Department").agg(collect_set("Salary")).show()

# Output:
# CSE → [5000,6000]
# IT  → [7000,4000]


# countDistinct()
df.select(countDistinct("Department")).show()

# Output:
# 2


# count()
df.select(count("Salary")).show()

# Output:
# 5


# first()
df.select(first("Salary")).show()

# Output:
# 5000


# last()
df.select(last("Salary")).show()

# Output:
# 5000


# max()
df.select(max("Salary")).show()

# Output:
# 7000


# min()
df.select(min("Salary")).show()

# Output:
# 4000


# sum()
df.select(sum("Salary")).show()

# Output:
# 27000

spark.stop()