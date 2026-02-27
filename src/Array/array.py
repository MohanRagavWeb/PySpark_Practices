from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ArrayFunctions").getOrCreate()

data = [
    ("Mohan", ["Python", "Spark", "SQL"]),
    ("Priya", ["Java", "SQL"]),
    ("Rahul", ["Spark", "Hadoop"])
]

df = spark.createDataFrame(data, ["Name", "Skills"])

df.show()

# Output:
# +------+----------------------+
# |Name  |Skills                |
# +------+----------------------+
# |Mohan |[Python, Spark, SQL]  |
# |Priya |[Java, SQL]           |
# |Rahul |[Spark, Hadoop]       |
# +------+----------------------+


# 1️⃣ ARRAY()
df.select(array("Name")).show()

# Output:
# Creates array column with Name


# 2️⃣ ARRAY_CONTAINS()
df.select("Name", array_contains("Skills", "Spark")).show()

# Output:
# +------+------------------------+
# |Name  |array_contains(Skills..)|
# +------+------------------------+
# |Mohan | true                   |
# |Priya | false                  |
# |Rahul | true                   |
# +------+------------------------+


# 3️⃣ ARRAY_LENGTH() -> size()
df.select("Name", size("Skills")).show()

# Output:
# +------+-------------+
# |Name  |size(Skills) |
# +------+-------------+
# |Mohan |      3      |
# |Priya |      2      |
# |Rahul |      2      |
# +------+-------------+


# 4️⃣ ARRAY_POSITION()
df.select("Name", array_position("Skills", "Spark")).show()

# Output:
# +------+--------------------------+
# |Name  |array_position(Skills...) |
# +------+--------------------------+
# |Mohan |            2             |
# |Priya |            0             |
# |Rahul |            1             |
# +------+--------------------------+


# 5️⃣ ARRAY_REMOVE()
df.select("Name", array_remove("Skills", "SQL")).show()

# Output:
# +------+-------------------+
# |Name  |array_remove       |
# +------+-------------------+
# |Mohan |[Python, Spark]    |
# |Priya |[Java]             |
# |Rahul |[Spark, Hadoop]    |
# +------+-------------------+

spark.stop()