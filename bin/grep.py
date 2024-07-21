from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("PySpark Grep") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .getOrCreate()

# Define the pattern to search for
pattern = "Neo4j"

# Read the input text file into a DataFrame
input_file = "hdfs://localhost:9000/user/root/input/numbers.txt"
output_file = "hdfs://localhost:9000/user/root/output/numbers.txt"

df = spark.read.text(input_file)
df.write.mode("overwrite").text(output_file)

# Filter rows that contain the pattern
filtered_df = df.filter(col("value").contains(pattern))

# Show the results
df.saveAsTextFile(output_file)

# Stop the SparkSession
spark.stop()
