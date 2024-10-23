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
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.lineage.Conversions._
import org.apache.spark.rdd.lineage.LineageContext

object TweetLineage {

  implicit val formats: Formats = DefaultFormats
  case class Movie(text: String)

  def main(args: Array[String]): Unit = {
    // Create Spark configuration and context
    val conf = new SparkConf().setAppName("TweetLineage")
    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    // Define input and output paths
    val inputPath = "hdfs://namenode:9000/user/root/input/" + args(0)
    val outputPath = "hdfs://namenode:9000/user/root/output/" + args(1)
    val movieTerm = List("badboys", "inception", "gangstar")

    // Read input file
    val inputRdd = lc.textFile(inputPath, 30, false)

    // filter by tags
    val hashTags = List("#movie", "#film", "#cinema", "#hollywood", "#blockbuster")
    val filteredRdd = inputRdd.filter(m => hashTags.exists(tag => m.contains(tag)))

    // get the tweet text
    val textRdd = filteredRdd.map(line => parse(line).extract[Movie].text.toLowerCase())

    // extract the movie name
    val movieTextRdd = textRdd.map(m => (movieTerm.find(t => m.contains(t)), m))

    // categorize
    val goodAdjectives = List("incredible", "great", "top-notch", "masterpiece", "perfect")
    val badAdjectives = List("confusing", "overrated", "unsatisfied", "poor", "bad", "hard")
    val ratedRdd = movieTextRdd.map(m => (m._1.getOrElse(""),
        (if (goodAdjectives.exists(a => m._2.contains(a))) 1 else 0,
        if (badAdjectives.exists(a => m._2.contains(a))) 1 else 0)))

    // count
    ratedRdd.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    ratedRdd.saveAsTextFile(outputPath)

    // Stop the Spark context
    sc.stop()
  }
}
