import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType

@pandas_udf(IntegerType())
def bonus_udf(salary: pd.Series) -> pd.Series:
    return salary + 1000

spark = SparkSession.builder.appName("panda UDF").getOrCreate()
data=[
    ("Mohan",500)
]
df=spark.createDataFrame(data,["name","Salary"])
df.withColumn("NewSalary", bonus_udf("Salary")).show()