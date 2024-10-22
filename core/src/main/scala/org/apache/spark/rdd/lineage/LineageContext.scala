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

package org.apache.spark.rdd.lineage

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, TextInputFormat}

import org.apache.spark._
import org.apache.spark.util.SerializableConfiguration

class LineageContext(@transient val sparkContext: SparkContext) extends Serializable {

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   * The text files must be encoded as UTF-8.
   *
   * @param path path to the text file on a supported file system
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of lines of the text file
   */
  def textFile(path: String, minPartitions: Int): Lineage[String] = {
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }
  def textFile(path: String): Lineage[String] = {
    textFile(path, sparkContext.defaultMinPartitions)
  }
  def textFile(path: String, minPartitions: Int, detailed: Boolean): Lineage[String] = {
    val rdd = textFile(path, minPartitions)
    rdd.detailed = detailed
    rdd
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param inputFormatClass storage format of the data to be read
   * @param keyClass `Class` of the key associated with the `inputFormatClass` parameter
   * @param valueClass `Class` of the value associated with the `inputFormatClass` parameter
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of tuples of key and corresponding value
   */
  def hadoopFile[K, V](
                        path: String,
                        inputFormatClass: Class[_ <: InputFormat[K, V]],
                        keyClass: Class[K],
                        valueClass: Class[V],
                        minPartitions: Int = sparkContext.defaultMinPartitions): Lineage[(K, V)] = {
    // This is a hack to enforce loading hdfs-site.xml.
    // See SPARK-11227 for details.
    FileSystem.getLocal(sparkContext.hadoopConfiguration)

    // A Hadoop configuration can be about 10 KiB, which is pretty big, so broadcast it.
    val confBroadcast = sparkContext
      .broadcast(new SerializableConfiguration(sparkContext.hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
    new HadoopLRDD(
      this,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions,
      name = "Read", description = s"Read from $path").setName(path)
  }
}
