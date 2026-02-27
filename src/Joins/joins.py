from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JoinsExample").getOrCreate()

# -------------------------------
# Create Employees DataFrame
# -------------------------------

emp_data = [
    (1, "Mohan"),
    (2, "Priya"),
    (3, "Rahul"),
    (4, "Anu")
]

emp_df = spark.createDataFrame(emp_data, ["emp_id", "name"])

# -------------------------------
# Create Department DataFrame
# -------------------------------

dept_data = [
    (1, "CSE"),
    (2, "IT"),
    (5, "ECE")
]

dept_df = spark.createDataFrame(dept_data, ["emp_id", "dept"])


# ===============================
# 1️⃣ INNER JOIN
# ===============================

emp_df.join(dept_df, "emp_id", "inner").show()

# Output:
# +------+-------+----+
# |emp_id| name  |dept|
# +------+-------+----+
# |   1  | Mohan |CSE |
# |   2  | Priya |IT  |
# +------+-------+----+


# ===============================
# 2️⃣ CROSS JOIN
# ===============================

emp_df.crossJoin(dept_df).show()

# Output (4 × 3 = 12 rows)
# Every employee combined with every department


# ===============================
# 3️⃣ FULL OUTER JOIN
# ===============================

emp_df.join(dept_df, "emp_id", "outer").show()

# Output:
# +------+-------+----+
# |emp_id| name  |dept|
# +------+-------+----+
# |   1  | Mohan |CSE |
# |   2  | Priya |IT  |
# |   3  | Rahul |null|
# |   4  | Anu   |null|
# |   5  | null  |ECE |
# +------+-------+----+


# ===============================
# 4️⃣ LEFT JOIN
# ===============================

emp_df.join(dept_df, "emp_id", "left").show()

# Output:
# +------+-------+----+
# |emp_id| name  |dept|
# +------+-------+----+
# |   1  | Mohan |CSE |
# |   2  | Priya |IT  |
# |   3  | Rahul |null|
# |   4  | Anu   |null|
# +------+-------+----+


# ===============================
# 5️⃣ RIGHT JOIN
# ===============================

emp_df.join(dept_df, "emp_id", "right").show()

# Output:
# +------+-------+----+
# |emp_id| name  |dept|
# +------+-------+----+
# |   1  | Mohan |CSE |
# |   2  | Priya |IT  |
# |   5  | null  |ECE |
# +------+-------+----+


# ===============================
# 6️⃣ LEFT SEMI JOIN
# ===============================

emp_df.join(dept_df, "emp_id", "left_semi").show()

# Output:
# +------+-------+
# |emp_id| name  |
# +------+-------+
# |   1  | Mohan |
# |   2  | Priya |
# +------+-------+


# ===============================
# 7️⃣ LEFT ANTI JOIN
# ===============================

emp_df.join(dept_df, "emp_id", "left_anti").show()

# Output:
# +------+-------+
# |emp_id| name  |
# +------+-------+
# |   3  | Rahul |
# |   4  | Anu   |
# +------+-------+


spark.stop()