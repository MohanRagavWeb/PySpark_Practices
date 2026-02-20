import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"



from pyspark.sql import SparkSession

# Step 1: Create SparkSession
spark = SparkSession.builder \
        .appName("Student Data") \
        .getOrCreate()

# Step 2: Create data
data = [("Mohan", 23), ("Priya", 22), ("Rahul", 24)]

# Step 3: Convert to DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

# Step 4: Show data
df.show()

# Step 5: Stop session
spark.stop()