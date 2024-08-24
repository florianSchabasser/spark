import org.apache.spark.rdd.lineage.LineageContext
import org.apache.spark.rdd.lineage.Conversions._

val inputPath = "hdfs://localhost:9000/user/root/input/logs.txt"
val outputPath = "hdfs://localhost:9000/user/root/output/logs_filtered.txt"
val searchTerm = "Neo4j"

val lc = new LineageContext(sc)
val inputRDD = lc.textFile(inputPath)

val matchingLines = inputRDD.filter(line => line.contains(searchTerm))

matchingLines.saveAsTextFile(outputPath)
