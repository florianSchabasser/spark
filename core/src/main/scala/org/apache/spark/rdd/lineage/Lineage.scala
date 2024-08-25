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

import scala.reflect.ClassTag

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.TextOutputFormat

import org.apache.spark.TaskContext
import org.apache.spark.lineage.{ILineageApi, ILineageGetter, LineageApi}
import org.apache.spark.rdd.RDD

trait Lineage[T] extends RDD[T] {

  implicit def tTag: ClassTag[T]
  @transient def lineageContext: LineageContext

  /** Globally unique ID over multiple SparkContext */
  val nodeId: String = s"${sparkContext.applicationId}#${id}"
  /** Determine whether the value should be transferred with the lineage or not */
  private val transferValue: String = sparkContext.getConf.get("spark.rdd.intermediateResults")
  private[spark] var generateHashOut: T => String = LineageHashUtil.getUUIDHashOut
  protected var _term: String = _
  protected var _description: String = _

  def lineage(value: T, context: TaskContext): T = {
    val hashOut: String = generateHashOut(value)

    // Use partitionId as message key, to process partitions in parallel on backend side
    // but sequential within a task - Retries will write to the same kafka partition

      lineage().capture(s"${nodeId}#${context.getRecordId}",
      context.getFlowHash(), hashOut, extractValue(value))
    context.setFlowHash(hashOut)

    value
  }

  def lineage(): ILineageApi = {
    LineageApi.get.withName(_term).withDescription(_description)
  }

  def withDescription(description: String): Lineage[T] = {
    _description = description
    LineageApi.get.register(nodeId, _term, _description)
    this
  }

  /** Returns the first parent RDD */
  override protected[spark] def firstParent[U: ClassTag]: Lineage[U] =
    dependencies.head.rdd.asInstanceOf[Lineage[U]]

  protected def extractValue(value: T): String = {
    if (transferValue != null && transferValue.toBoolean) {
      return value match {
        case getter: ILineageGetter => getter.getValue
        case tuple: (?, ?) => s"${tuple._2}"
        case plain: String => plain
        case any => any.toString
      }
    }
    null
  }

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  override def filter(f: T => Boolean): Lineage[T] = {
    val cleanF = sparkContext.clean(f)
    new MapPartitionsLRDD[T, T](
      this,
      (_, _, iter) => iter.filter(cleanF),
      preservesPartitioning = true,
      term = "Filter")
  }

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  override def map[U: ClassTag](f: T => U): Lineage[U] = {
    val cleanF = sparkContext.clean(f)
    new MapPartitionsLRDD[U, T](this, (_, _, iter) => iter.map(cleanF),
      term = "Map")
  }


  /**
   * Return a new RDD by first applying a function to all elements of this
   * RDD, and then flattening the results.
   */
  override def flatMap[U: ClassTag](f: T => TraversableOnce[U]): Lineage[U] = {
    val cleanF = sparkContext.clean(f)
    new FlatMapPartitionsLRDD[U, T](this, (_, _, iter) => iter.flatMap(cleanF),
      term = "FlatMap")
  }

  private def persist[U: ClassTag](f: Iterator[T] => Iterator[U],
                                   term: String = "Save",
                                   description: String = null): Lineage[U] = {
    val cleanedF = sparkContext.clean(f)
    new PersistLRDD(
      this,
      (_: TaskContext, _: Int, iter: Iterator[T]) => cleanedF(iter),
      term = term, description = description)
  }

  /**
   * Save this RDD as a compressed text file, using string representations of elements.
   */
  override def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit = {
    val rdd = this.persist({ iter =>
      val text = new Text()
      iter.map { x =>
        require(x != null, "text files do not allow null rows")
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }, description = s"Save to ${path}")
    rdd.saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path, codec)
  }
}
