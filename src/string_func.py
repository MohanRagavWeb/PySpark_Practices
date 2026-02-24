import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"




from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("StringFunctions").getOrCreate()

data = [
    (" mohan kumar ", "CSE-2024", "hello world"),
    (" PRIYA ", "IT-2023", "spark learning"),
    ("rahul", "ECE-2022", "big data")
]

df = spark.createDataFrame(data, ["Name", "Code", "Text"])

# upper()
df.select(upper("Name")).show()

# Output:
# MOHAN KUMAR
# PRIYA
# RAHUL


# trim()
df.select(trim("Name")).show()

# Output:
# mohan kumar
# PRIYA
# rahul


# ltrim()
df.select(ltrim("Name")).show()


# rtrim()
df.select(rtrim("Name")).show()


# substring_index()
df.select(substring_index("Code", "-", 1)).show()

# Output:
# CSE
# IT
# ECE


# substring()
df.select(substring("Name", 1, 5)).show()


# split()
df.select(split("Code", "-")).show()

# Output:
# [CSE, 2024]
# [IT, 2023]
# [ECE, 2022]


# repeat()
df.select(repeat("Name", 2)).show()


# rpad()
df.select(rpad("Name", 15, "*")).show()


# lpad()
df.select(lpad("Name", 15, "*")).show()


# regex_replace()
df.select(regex_replace("Text", " ", "_")).show()

# Output:
# hello_world
# spark_learning
# big_data


# lower()
df.select(lower("Name")).show()


# regex_extract()
df.select(regex_extract("Code", "[A-Z]+", 0)).show()

# Output:
# CSE
# IT
# ECE


# length()
df.select(length("Name")).show()


# instr()
df.select(instr("Text", "spark")).show()

# Output:
# 0
# 1
# 0


# initcap()
df.select(initcap("Text")).show()

# Output:
# Hello World
# Spark Learning
# Big Data

spark.stop()