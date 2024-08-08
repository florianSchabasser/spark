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

import org.apache.spark._
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.serializer.Serializer

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion.
 */
private[spark] class PairLRDDFunctions[K, V](self: Lineage[(K, V)])
                             (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctions[K, V](self) {

  def lineageContext: LineageContext = self.lineageContext

  var term: String = _

  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
   *
   * Users provide three functions:
   *
   *  - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   *  - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   *  - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD, and whether to perform
   * map-side aggregation (if a mapper can produce multiple items with the same key).
   *
   * @note V and C can be different -- for example, one might group an RDD of type
   * (Int, Int) into an RDD of type (Int, Seq[Int]).
   */
  override def combineByKeyWithClassTag[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null)(implicit ct: ClassTag[C]): Lineage[(K, C)] = {
    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
    if (keyClass.isArray) {
      if (mapSideCombine) {
        throw SparkCoreErrors.cannotUseMapSideCombiningWithArrayKeyError()
      }
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw SparkCoreErrors.hashPartitionerCannotPartitionArrayKeyError()
      }
    }
    val aggregator = new Aggregator[K, V, C](
      self.context.clean(createCombiner),
      self.context.clean(mergeValue),
      self.context.clean(mergeCombiners))

    self.generateHashOut = LineageHashUtil.getKeyHashOut(self)
    new ShuffledLRDD[K, V, C](self, partitioner, term)
      .setSerializer(serializer)
      .setAggregator(aggregator)
      .asInstanceOf[ShuffledLRDD[K, V, C]]
      .setMapSideCombine(mapSideCombine)
  }

  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce.
   */
  override def reduceByKey(partitioner: Partitioner, func: (V, V) => V): Lineage[(K, V)] = {
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
  }

  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.
   */
  override def reduceByKey(func: (V, V) => V, numPartitions: Int): Lineage[(K, V)] = {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }

  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
   * parallelism level.
   */
  override def reduceByKey(func: (V, V) => V): Lineage[(K, V)] = {
    this.term = "reduceByKey"
    reduceByKey(defaultPartitioner(self), func)
  }
}
