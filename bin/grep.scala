val input_file: String = "hdfs://localhost:9000/user/root/input/numbers.txt"
val output_file: String = "hdfs://localhost:9000/user/root/output/numbers.txt"

val data=sc.textFile(input_file)

val filtered_df = data.filter(row => row.toInt > 300, description = "Value > 300")

filtered_df.saveAsTextFile(output_file)
