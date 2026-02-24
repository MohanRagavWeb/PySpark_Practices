import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"



from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("General DF Functions").getOrCreate()

# Create sample data
data = [
    ("Mohan", 23, "CSE"),
    ("Priya", 22, "IT"),
    ("Rahul", 24, "CSE"),
    ("Anu", 21, "ECE"),
    ("Mohan", 23, "CSE")
]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Age", "Department"])


# 1️⃣ show()
df.show()

# Output:
# +------+---+-----------+
# | Name |Age|Department |
# +------+---+-----------+
# | Mohan|23 |CSE        |
# | Priya|22 |IT         |
# | Rahul|24 |CSE        |
# | Anu  |21 |ECE        |
# | Mohan|23 |CSE        |
# +------+---+-----------+


# 2️⃣ collect()
print(df.collect())

# Output:
# [Row(Name='Mohan', Age=23, Department='CSE'),
#  Row(Name='Priya', Age=22, Department='IT'),
#  Row(Name='Rahul', Age=24, Department='CSE'),
#  Row(Name='Anu', Age=21, Department='ECE'),
#  Row(Name='Mohan', Age=23, Department='CSE')]


# 3️⃣ take()
print(df.take(2))

# Output:
# [Row(Name='Mohan', Age=23, Department='CSE'),
#  Row(Name='Priya', Age=22, Department='IT')]


# 4️⃣ printSchema()
df.printSchema()

# Output:
# root
#  |-- Name: string
#  |-- Age: long
#  |-- Department: string


# 5️⃣ count()
print("Total rows:", df.count())

# Output:
# Total rows: 5


# 6️⃣ select()
df.select("Name").show()

# Output:
# +------+
# | Name |
# +------+
# | Mohan|
# | Priya|
# | Rahul|
# | Anu  |
# | Mohan|
# +------+


# 7️⃣ filter()
df.filter(df.Age > 22).show()

# Output:
# +------+---+-----------+
# | Name |Age|Department |
# +------+---+-----------+
# | Mohan|23 |CSE        |
# | Rahul|24 |CSE        |
# | Mohan|23 |CSE        |
# +------+---+-----------+


# 8️⃣ where()
df.where(df.Department == "CSE").show()

# Output:
# +------+---+-----------+
# | Name |Age|Department |
# +------+---+-----------+
# | Mohan|23 |CSE        |
# | Rahul|24 |CSE        |
# | Mohan|23 |CSE        |
# +------+---+-----------+


# 9️⃣ like()
df.filter(df.Name.like("M%")).show()

# Output:
# +------+---+-----------+
# | Name |Age|Department |
# +------+---+-----------+
# | Mohan|23 |CSE        |
# | Mohan|23 |CSE        |
# +------+---+-----------+


# 🔟 sort()
df.sort("Age").show()

# Output:
# +------+---+-----------+
# | Name |Age|Department |
# +------+---+-----------+
# | Anu  |21 |ECE        |
# | Priya|22 |IT         |
# | Mohan|23 |CSE        |
# | Mohan|23 |CSE        |
# | Rahul|24 |CSE        |
# +------+---+-----------+


# 11️⃣ describe()
df.describe().show()

# Output:
# +-------+-----+------------------+-----------+
# |summary|Name |Age               |Department |
# +-------+-----+------------------+-----------+
# |count  |5    |5                 |5          |
# |mean   |null |22.6              |null       |
# |stddev |null |1.140175425099138 |null       |
# |min    |Anu  |21                |CSE        |
# |max    |Rahul|24                |IT         |
# +-------+-----+------------------+-----------+


# 12️⃣ columns
print(df.columns)

# Output:
# ['Name', 'Age', 'Department']


# 13️⃣ first()
print(df.first())

# Output:
# Row(Name='Mohan', Age=23, Department='CSE')


# 14️⃣ distinct()
df.distinct().show()

# Output:
# Duplicate Mohan row removed


# 15️⃣ drop()
df.drop("Department").show()

# Output:
# +------+---+
# | Name |Age|
# +------+---+
# | Mohan|23 |
# | Priya|22 |
# | Rahul|24 |
# | Anu  |21 |
# | Mohan|23 |
# +------+---+


# 16️⃣ withColumn()
df.withColumn("AgePlusOne", df.Age + 1).show()

# Output:
# New column AgePlusOne added


# 17️⃣ withColumnRenamed()
df.withColumnRenamed("Age", "StudentAge").show()

# Output:
# Column Age renamed to StudentAge


# 18️⃣ dropDuplicates()
df.dropDuplicates().show()

# Output:
# Duplicate Mohan record removed


spark.stop()