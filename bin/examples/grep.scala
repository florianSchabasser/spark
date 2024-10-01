// docker exec -it 29d5515597fe spark-shell --master spark://spark-master:7077 --conf "spark.rdd.intermediateResults=true"

import org.apache.spark.rdd.lineage.LineageContext
import org.apache.spark.rdd.lineage.Conversions._

val inputPath = "hdfs://namenode:9000/user/root/input/logs.txt"
val outputPath = "hdfs://namenode:9000/user/root/output/logs_filtered.txt"
val searchTerm = "word"

val lc = new LineageContext(sc)
val inputRDD = lc.textFile(inputPath)

val matchingLines = inputRDD.filter(line => line.contains(searchTerm))

matchingLines.saveAsTextFile(outputPath)
