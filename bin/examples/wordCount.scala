// docker exec -it 63ca0cddf2cc spark-shell --master spark://spark-master:7077 --conf "spark.rdd.intermediateResults=true"

import org.apache.spark.rdd.lineage.LineageContext
import org.apache.spark.rdd.lineage.Conversions._

val inputPath = "hdfs://namenode:9000/user/root/input/words_small.txt"
val outputPath = "hdfs://namenode:9000/user/root/output/word_count.txt"

val lc = new LineageContext(sc)
val inputRDD = lc.textFile(inputPath)

val splitData = inputRDD.flatMap(l => l.split(" ")).withDescription("Split words by space")
val mapdata = splitData.map(w => (w,1)).withDescription("Create pairs (word,1)")
val result = mapdata.reduceByKey(_+_)

result.saveAsTextFile(outputPath)

