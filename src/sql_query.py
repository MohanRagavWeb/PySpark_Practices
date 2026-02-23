
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"



from pyspark.sql import SparkSession

# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("SQL Integration Example") \
    .getOrCreate()

# Step 2: Create sample data
data = [
    ("Mohan", "CSE", 23),
    ("Priya", "IT", 22),
    ("Rahul", "CSE", 24),
    ("Anu", "ECE", 21)
]

# Step 3: Create DataFrame
df = spark.createDataFrame(data, ["Name", "Department", "Age"])

print("Original Data:")
df.show()

# Step 4: Create Temporary View
df.createOrReplaceTempView("students")

print("SQL Query on Temp View:")
spark.sql("""
    SELECT Name, Department
    FROM students
    WHERE Age > 22
""").show()

# Step 5: Create Global Temporary View
df.createGlobalTempView("students_global")

print("SQL Query on Global Temp View:")
spark.sql("""
    SELECT Department, COUNT(*) as total_students
    FROM global_temp.students_global
    GROUP BY Department
""").show()

# Step 6: Stop Spark
spark.stop()