import org.apache.spark.rdd.lineage.LineageContext
import org.apache.spark.rdd.lineage.Conversions._

val input_file: String = "hdfs://localhost:9000/user/root/input/words_small.txt"
val output_file: String = "hdfs://localhost:9000/user/root/output/wordCount.csv"

val lc = new LineageContext(sc)
val data=lc.textFile(input_file)

val splitdata = data.flatMap(l => l.split(" "))
val mapdata = splitdata.map(w => (w,1))
val reducedata = mapdata.reduceByKey(_+_)

reducedata.saveAsTextFile(output_file)