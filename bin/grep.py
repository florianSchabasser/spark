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
input_file = "logs.txt"
df = spark.read.text(input_file)

# Filter rows that contain the pattern
filtered_df = df.filter(col("value").contains(pattern))

# Show the results
filtered_df.show(truncate=False)

# Stop the SparkSession
spark.stop()
