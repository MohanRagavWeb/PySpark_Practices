from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("NestedSchema").getOrCreate()

schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("address", StructType([
        StructField("city", StringType()),
        StructField("pincode", IntegerType())
    ])),
    StructField("skills", ArrayType(StringType())),
    StructField("projects", ArrayType(
        StructType([
            StructField("title", StringType()),
            StructField("year", IntegerType())
        ])
    ))
])

data = [
    ("Mohan", 23, ("Chennai", 635109), ["Python", "Spark"],
     [("AI", 2024), ("ML", 2025)])
]

df = spark.createDataFrame(data, schema)

df.printSchema()
df.show(truncate=False)