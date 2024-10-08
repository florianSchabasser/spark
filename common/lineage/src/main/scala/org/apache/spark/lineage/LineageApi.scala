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

package org.apache.spark.lineage

import org.apache.spark.internal.Logging
import org.apache.spark.lineage.dto.{LFlow, LNodeLink, LNodeRegistration}

class LineageApi extends ILineageApi with Logging {

  private[spark] val dispatcher = new LineageDispatcher()

  override def register(nodeId: String, name: String, description: String): Unit = {
    val lNodeRegistration: LNodeRegistration = LNodeRegistration(nodeId, name, description)
    dispatcher.register(LineageApi.messageKey.get(), lNodeRegistration)
  }

  override def flowLink(srcNodeId: String, destNodeId: String): Unit = {
    if (srcNodeId != null && destNodeId != null) dispatcher
      .link(LineageApi.messageKey.get(), LNodeLink(srcNodeId, destNodeId))
  }

  override def capture(flowId: String, hashIn: String, hashOut: String,
                       value: String = null): Unit = {
    dispatcher.capture(LineageApi.messageKey.get(), LFlow(flowId, hashIn, hashOut,
      LineageApi.name.get(), LineageApi.description.get(), value))
  }

  override def capture(flowId: String, hashIn: String, hashOut: String): Unit = {
    dispatcher.capture(LineageApi.messageKey.get(), new LFlow(flowId, hashIn, hashOut))
  }

  override def withName(name: String): ILineageApi = {
    LineageApi.name.set(name)
    this
  }

  override def withDescription(description: String): ILineageApi = {
    LineageApi.description.set(description)
    this
  }

}

// Driver / Worker instance (one)
object LineageApi {

  // Use partitionId as message key, to process partitions in parallel on backend side
  // but sequential within a task - Retries will write to the same kafka partition
  private[spark] val messageKey: ThreadLocal[String] = ThreadLocal.withInitial(() => "driver")
  private[spark] val name: ThreadLocal[String] = new ThreadLocal
  private[spark] val description: ThreadLocal[String] = new ThreadLocal

  private val apiInstance: ILineageApi = new LineageApi()

  def get: ILineageApi = apiInstance

}
