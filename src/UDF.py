from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *

spark = SparkSession.builder.appName("UDFExample").getOrCreate()

data = [
    ("Mohan", 5000),
    ("Priya", 7000),
    ("Rahul", 4000)
]

df = spark.createDataFrame(data, ["Name", "Salary"])

# UDF function
def category(salary):
    if salary >= 6000:
        return "High"
    else:
        return "Low"

category_udf = udf(category, StringType())

df.withColumn("SalaryCategory", category_udf("Salary")).show()

# Output:
# +------+-------+--------------+
# |Name  |Salary |SalaryCategory|
# +------+-------+--------------+
# |Mohan |5000   |Low           |
# |Priya |7000   |High          |
# |Rahul |4000   |Low           |
# +------+-------+--------------+

spark.stop()