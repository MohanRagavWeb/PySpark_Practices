from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CastFunction").getOrCreate()

data = [
    ("Mohan", "23", "5000.50"),
    ("Priya", "22", "6000.75")
]

df = spark.createDataFrame(data, ["Name", "Age", "Salary"])

df.printSchema()

# Output:
# Age → string
# Salary → string


# 1️⃣ Cast Age to Integer
df = df.withColumn("Age", col("Age").cast("int"))

# 2️⃣ Cast Salary to Double
df = df.withColumn("Salary", col("Salary").cast("double"))

df.printSchema()

# Output:
# Age → integer
# Salary → double


# 3️⃣ Cast integer to string
df = df.withColumn("AgeString", col("Age").cast("string"))

df.show()

# Output:
# +------+---+-------+---------+
# | Name |Age|Salary |AgeString|
# +------+---+-------+---------+
# | Mohan|23 |5000.5 | "23"    |
# | Priya|22 |6000.75| "22"    |
# +------+---+-------+---------+

spark.stop()