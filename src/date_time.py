from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DateTimeFunctions").getOrCreate()

data = [
    ("Mohan", "2024-01-15"),
    ("Priya", "2023-12-10"),
    ("Rahul", "2022-08-05")
]

df = spark.createDataFrame(data, ["Name", "JoinDate"])


# Convert string to date
df = df.withColumn("JoinDate", to_date("JoinDate"))


# CURRENT_DATE()
df.select(current_date()).show()

# Output:
# 2026-02-24   (example - current system date)


# CURRENT_TIMESTAMP()
df.select(current_timestamp()).show()

# Output:
# 2026-02-24 16:45:30   (example)


# DATE_ADD()
df.select(date_add("JoinDate", 10)).show()

# Output:
# +----------------------+
# |date_add(JoinDate,10) |
# +----------------------+
# |2024-01-25            |
# |2023-12-20            |
# |2022-08-15            |
# +----------------------+


# DATEDIFF()
df.select(datediff(current_date(), "JoinDate")).show()

# Output:
# Days difference from today


# YEAR()
df.select(year("JoinDate")).show()

# Output:
# 2024
# 2023
# 2022


# MONTH()
df.select(month("JoinDate")).show()

# Output:
# 1
# 12
# 8


# DAY()
df.select(dayofmonth("JoinDate")).show()

# Output:
# 15
# 10
# 5


# TO_DATE()
df.select(to_date("JoinDate")).show()

# Output:
# Converted date format


# DATE_FORMAT()
df.select(date_format("JoinDate", "dd-MM-yyyy")).show()

# Output:
# 15-01-2024
# 10-12-2023
# 05-08-2022

spark.stop()