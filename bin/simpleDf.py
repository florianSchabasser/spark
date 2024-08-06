from pyspark.sql.functions import col

df = spark.read.text("numbers.txt")
# Convert the value column to integer
df = df.withColumn("stat", df["value"].cast("int"))
# Filter rows where the value is greater than 2
filtered_df = df.filter(col("value") > 300)
# Show the filtered data
filtered_df.show()
