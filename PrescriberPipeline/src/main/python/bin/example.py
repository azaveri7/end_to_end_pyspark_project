from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

# Create a simple Spark DataFrame
data = [("Alice", 1), ("Bob", 2)]
columns = ["Name", "Id"]
df_spark = spark.createDataFrame(data, columns)

# Convert to Pandas DataFrame
df_pandas = df_spark.limit(10).toPandas()

print(df_pandas)
