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

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.lineage.LineageApi
import org.apache.spark.rdd.MapPartitionsRDD

private[spark] class PersistLRDD[U: ClassTag, T: ClassTag](
    prev: Lineage[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false,
    name: String = "PersistLRDD", description: String = null)
  extends MapPartitionsRDD[U, T](prev, f, preservesPartitioning, isFromBarrier, isOrderSensitive)
      with Lineage[U] {

  _name = name
  _description = description
  capture = true
  LineageApi.instance.register(nodeId, _name, _description)
  LineageApi.instance.flowLink(prev.nodeId, nodeId)

  override def tTag: ClassTag[U] = classTag[U]
  override def lineageContext: LineageContext = prev.lineageContext

  override def lineage(value: U, context: TaskContext): U = {
    generateHashOut = LineageHashUtil.getHashOutPersist(context)

    super.lineage(value, context)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    super.compute(split, context).map(v => lineage(v, context))
  }
}
