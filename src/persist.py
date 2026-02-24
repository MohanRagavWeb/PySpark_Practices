
import os

os.environ["PYSPARK_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Mohan\AppData\Local\Programs\Python\Python311\python.exe"


from pyspark.sql import SparkSession
from pyspark import StorageLevel

# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("PersistExample") \
    .getOrCreate()

# Step 2: Create sample data
data = [
    ("Mohan", 23),
    ("Priya", 22),
    ("Rahul", 24),
    ("Anu", 21),
    ("Kiran", 25)
]

# Step 3: Create DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

print("Original Data:")
df.show()

# Step 4: Persist DataFrame in Memory + Disk
df.persist(StorageLevel.MEMORY_AND_DISK)

# Step 5: First operation (Data gets computed and stored)
print("Filter Age > 22:")
df.filter(df.Age > 22).show()

# Step 6: Second operation (Uses persisted data, faster)
print("Group by Age:")
df.groupBy("Age").count().show()

# Step 7: Remove persisted data
df.unpersist()

spark.stop()