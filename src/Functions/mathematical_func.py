from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("MathFunctions").getOrCreate()

data = [
    ("A", 16),
    ("B", 25),
    ("C", -9),
    ("D", 2)
]

df = spark.createDataFrame(data, ["Name", "Value"])

df.show()

# Output:
# +----+-----+
# |Name|Value|
# +----+-----+
# | A  | 16  |
# | B  | 25  |
# | C  | -9  |
# | D  |  2  |
# +----+-----+


# 1️⃣ ABS()
df.select(abs("Value")).show()

# Output:
# 16
# 25
# 9
# 2


# 2️⃣ CEIL()
df.select(ceil("Value")).show()

# Output:
# 16
# 25
# -9
# 2


# 3️⃣ FLOOR()
df.select(floor("Value")).show()

# Output:
# 16
# 25
# -9
# 2


# 4️⃣ EXP()
df.select(exp("Value")).show()

# Output:
# e^16
# e^25
# e^-9
# e^2


# 5️⃣ LOG()
df.select(log("Value")).show()

# Output:
# log(16)
# log(25)
# null (for negative value)
# log(2)


# 6️⃣ POWER()
df.select(power("Value", 2)).show()

# Output:
# 256
# 625
# 81
# 4


# 7️⃣ SQRT()
df.select(sqrt("Value")).show()

# Output:
# 4
# 5
# null (sqrt of negative not possible)
# 1.414

spark.stop()