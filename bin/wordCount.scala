val input_file: String = "hdfs://localhost:9000/user/root/input/words_small.txt"
val output_file: String = "hdfs://localhost:9000/user/root/output/wordCount.csv"

val data=sc.textFile(input_file)

// val splitdata = data.flatMap(withDescription(l => l.split(" "), "Split sentance at space"))
val splitdata = data.flatMap(l => l.split(" "))
// val mapdata = splitdata.map(withDescription(w => (w,1), "Create key/value pairs"))
val mapdata = splitdata.map(w => (w,1))
val reducedata = mapdata.reduceByKey(_+_)

reducedata.saveAsTextFile(output_file)

