// docker exec -it 63ca0cddf2cc spark-shell --master spark://spark-master:7077 --conf "spark.rdd.lineage.detailed=true"
//ssh -N  -L 9870:localhost:9870 \
//  -L 9001:localhost:9001 \
//  -L 8080:localhost:8080 \
//  -L 7474:localhost:7474 \
//  -L 7687:localhost:7687 \
//  -i "ssh_key.pem" ec2-user@ec2-3-75-186-87.eu-central-1.compute.amazonaws.com

import org.apache.spark.rdd.lineage.LineageContext
import org.apache.spark.rdd.lineage.Conversions._

val inputPath = "hdfs://localhost:9000/user/root/input/1MB.txt"
val outputPath = "hdfs://localhost:9000/user/root/output/words_counted.txt"

val lc = new LineageContext(sc)
val inputRDD = lc.textFile(inputPath)

val splitData = inputRDD.flatMap(l => l.split(" ")).withDescription("Split words by space")
val mapdata = splitData.map(w => (w,1)).withDescription("Create pairs (word,1)")
val result = mapdata.reduceByKey(_+_)

result.saveAsTextFile(outputPath)