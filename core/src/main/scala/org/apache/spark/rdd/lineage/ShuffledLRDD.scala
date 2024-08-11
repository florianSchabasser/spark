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

import org.apache.spark._
import org.apache.spark.lineage.LineageApi
import org.apache.spark.rdd.ShuffledRDD

private[spark] class ShuffledLRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient prev: Lineage[_ <: Product2[K, V]],
    part: Partitioner,
    term: String = "ShuffledLRDD", description: String = null)
  extends ShuffledRDD[K, V, C](prev, part)
  with Lineage[(K, C)] {

  private val _prevNodeId = prev.nodeId
  _term = term
  _description = description
  LineageApi.getInstance.register(nodeId, _term, _description)
  LineageApi.getInstance.flowLink(_prevNodeId, nodeId)

  override def tTag: ClassTag[(K, C)] = classTag[(K, C)]
  override def lineageContext: LineageContext = prev.lineageContext

  override def lineage(value: (K, C), context: TaskContext): (K, C) = {
    val hashOut: String = generateHashOut(value)
    context.setRecordId(value._1.toString)

    context.lineage.capture(s"${nodeId}#${context.getRecordId}",
      s"${_prevNodeId}#${value._1}", hashOut, extractValue(value))
    context.setFlowHash(hashOut)

    value
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    super.compute(split, context).map(v => lineage(v, context))
  }

  /** Set mapSideCombine flag for RDD's shuffle. */
  override def setMapSideCombine(mapSideCombine: Boolean): ShuffledLRDD[K, V, C] = {
    this.mapSideCombine = mapSideCombine
    this
  }
}
