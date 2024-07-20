from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("PySpark Word Count") \
    .getOrCreate()

# Read the input text file into a DataFrame
input_file = "path/to/your/input_file.txt"
df = spark.read.text(input_file)

# Split each line into words and explode into individual rows
words_df = df.select(explode(split(col("value"), "\\s+")).alias("word"))

# Group by words and count the occurrences
word_count_df = words_df.groupBy("word").count()

# Show the results
word_count_df.show(truncate=False)

# Stop the SparkSession
spark.stop()
