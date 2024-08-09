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

class LineageApi(clientId: String) extends ILineageApi with Logging {

  private[spark] val dispatcher = new LineageDispatcher()

  override def register(nodeId: String, name: String, description: String): Unit = {
    dispatcher.register(LNodeRegistration(nodeId, name, description))
  }

  override def flowLink(srcNodeId: String, destNodeId: String): Unit = {
    dispatcher.link(LNodeLink(srcNodeId, destNodeId))
  }

  override def capture(key: String, flowId: String, hashIn: String, hashOut: String,
                       value: String = null): Unit = {
    dispatcher.capture(key, LFlow(flowId, hashIn, hashOut, value))
  }
}

object LineageApi {

  private val apiInstance: ILineageApi = new LineageApi("LineageApiCompanion")

  def getInstance: ILineageApi = apiInstance
}
