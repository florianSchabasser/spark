/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.lineage.Conversions._
import org.apache.spark.rdd.lineage.LineageContext


object Grep {
  def main(args: Array[String]): Unit = {
    // Create Spark configuration and context
    val conf = new SparkConf().setAppName("Grep")
    val sc = new SparkContext(conf)

    // Define input and output paths
    val inputPath = "hdfs://namenode:9000/user/root/input/" + args(0)
    val outputPath = "hdfs://namenode:9000/user/root/output/" + args(1)
    val searchTerm = args(3)

    // Read input file
    val inputRDD = sc.textFile(inputPath, 30)

    // Filter lines containing the search term
    val matchingLines = inputRDD.filter(line => line.contains(searchTerm))

    // Save the filtered lines to the output file
    matchingLines.saveAsTextFile(outputPath)

    // Stop the Spark context
    sc.stop()
  }
}