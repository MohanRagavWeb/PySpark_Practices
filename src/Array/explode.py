from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ExplodeFunctions").getOrCreate()

data = [
    ("Mohan", ["Python", "Spark"]),
    ("Priya", ["Java", "SQL"]),
    ("Rahul", None)
]

df = spark.createDataFrame(data, ["Name", "Skills"])

df.show()

# Output:
# +------+------------------+
# |Name  |Skills            |
# +------+------------------+
# |Mohan |[Python, Spark]   |
# |Priya |[Java, SQL]       |
# |Rahul |null              |
# +------+------------------+


# 1️⃣ explode()
df.select("Name", explode("Skills")).show()

# Output:
# Rahul removed because null array


# 2️⃣ explode_outer()
df.select("Name", explode_outer("Skills")).show()

# Output:
# Rahul kept with null value


# 3️⃣ posexplode_outer()
df.select("Name", posexplode_outer("Skills")).show()

# Output:
# Shows position + values

spark.stop()