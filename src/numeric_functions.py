from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("NumericFunctions").getOrCreate()

data = [
    ("Mohan", 5000),
    ("Priya", 7000),
    ("Rahul", 6000),
    ("Anu", -4000)
]

df = spark.createDataFrame(data, ["Name", "Salary"])


# SUM()
df.select(sum("Salary")).show()

# Output:
# +-----------+
# |sum(Salary)|
# +-----------+
# |      14000|
# +-----------+


# AVG()
df.select(avg("Salary")).show()

# Output:
# +-----------+
# |avg(Salary)|
# +-----------+
# |    3500.0 |
# +-----------+


# MIN()
df.select(min("Salary")).show()

# Output:
# +-----------+
# |min(Salary)|
# +-----------+
# |      -4000|
# +-----------+


# MAX()
df.select(max("Salary")).show()

# Output:
# +-----------+
# |max(Salary)|
# +-----------+
# |       7000|
# +-----------+


# ROUND()
df.select(round("Salary", -3)).show()

# Output:
# +------------------+
# |round(Salary, -3) |
# +------------------+
# |              5000|
# |              7000|
# |              6000|
# |             -4000|
# +------------------+


# ABS()
df.select(abs("Salary")).show()

# Output:
# +------------+
# |abs(Salary) |
# +------------+
# |        5000|
# |        7000|
# |        6000|
# |        4000|
# +------------+

spark.stop()