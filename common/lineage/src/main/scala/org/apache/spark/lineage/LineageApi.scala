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

import java.util.UUID

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.lineage.LineageApi.registrations
import org.apache.spark.lineage.dto.{LFlow, LNodeLink, LNodeRegistration}

class LineageApi extends ILineageApi with Logging {

  private[spark] val messageKey = ThreadLocal.withInitial(() => "driver")
  private[spark] val dispatcher = new LineageDispatcher(UUID.randomUUID().toString)

  override def register(nodeId: String, name: String, description: String): Unit = {
    val lNodeRegistration: LNodeRegistration = LNodeRegistration(nodeId, name, description)
    registrations.put(nodeId, lNodeRegistration)
    dispatcher.register(messageKey.get(), lNodeRegistration)
  }

  override def flowLink(srcNodeId: String, destNodeId: String): Unit = {
    if (srcNodeId != null && destNodeId != null) dispatcher
      .link(messageKey.get(), LNodeLink(srcNodeId, destNodeId))
  }

  override def capture(flowId: String, hashIn: String, hashOut: String,
                       value: String = null): Unit = {
    val lNodeRegistration: LNodeRegistration = registrations
      .get(s"${flowId.split("#")(0)}#${flowId.split("#")(1)}").orNull
    dispatcher.capture(messageKey.get(), LFlow(flowId, hashIn, hashOut, lNodeRegistration.name,
      lNodeRegistration.description, value))
  }

  private[spark] def close(): Unit = dispatcher.close()
}

// Driver / Worker instance (one)
object LineageApi {

  val registrations: mutable.Map[String, LNodeRegistration] =
    mutable.Map[String, LNodeRegistration]()

  private val apiInstance: ILineageApi = new LineageApi()

  def getInstance: ILineageApi = apiInstance
}
