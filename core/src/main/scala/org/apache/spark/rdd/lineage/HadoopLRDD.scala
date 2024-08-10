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

import scala.reflect._

import org.apache.hadoop.mapred._

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.lineage.LineageApi
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.util.SerializableConfiguration

private[spark] class HadoopLRDD[K, V](@transient lc: LineageContext,
                       broadcastConf: Broadcast[SerializableConfiguration],
                       initLocalJobConfFuncOpt: Option[JobConf => Unit],
                       inputFormatClass: Class[_ <: InputFormat[K, V]],
                       keyClass: Class[K],
                       valueClass: Class[V],
                       minPartitions: Int,
                       term: String = "HadoopLRDD",
                       description: String = null)
  extends HadoopRDD[K, V](
    lc.sparkContext,
    broadcastConf,
    initLocalJobConfFuncOpt,
    inputFormatClass,
    keyClass,
    valueClass,
    minPartitions) with Lineage[(K, V)] {

  _term = term
  _description = description
  LineageApi.getInstance.register(nodeId, _term, _description)

  override def tTag: ClassTag[(K, V)] = classTag[(K, V)]
  override def lineageContext: LineageContext = lc

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    new InterruptibleIterator[(K, V)](context, super.compute(theSplit, context)
      .map(v => lineage(v, context)))
  }
}
