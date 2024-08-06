# Input and output file paths
input_file = "hdfs://localhost:9000/user/root/input/words_small.txt"
output_file = "hdfs://localhost:9000/user/root/output/wordCount.csv"

# Read the input text file into an RDD
text_rdd = sc.textFile(input_file)

# Split each line into words and flatten the result
words_rdd = text_rdd.flatMap(lambda line: line.split())

# Count occurrences of each word
word_count_rdd = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Save the result to the output file
word_count_rdd.saveAsTextFile(output_file)