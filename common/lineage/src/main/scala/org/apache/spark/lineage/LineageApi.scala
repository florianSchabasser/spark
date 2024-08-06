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

object LineageApi extends ILineageApi with Logging {

  override def register(nodeId: String, name: String, description: String): Unit = {
    LineageDispatcher.register(LNodeRegistration(nodeId, name, description))
  }

  override def commit(nodeId: String): Unit = {

  }

  override def flowLink(srcNodeId: String, destNodeId: String): Unit = {
    LineageDispatcher.link(LNodeLink(srcNodeId, destNodeId))
  }

  override def capture(nodeId: String, hashIn: String, hashOut: String): Unit = {
    capture(nodeId, hashIn, hashOut, null)
  }

  override def capture(nodeId: String, hashIn: String, hashOut: String, value: String): Unit = {
    LineageDispatcher.capture(LFlow(nodeId, hashIn, hashOut, value))
  }

  override def addInput(nodeId: String, hashIn: String, tag: String): Unit = {
    addInput(nodeId, hashIn, tag, null)
  }

  override def addInput(nodeId: String, hashIn: String, tag: String, value: String): Unit = {

  }

  override def addOutput(nodeId: String, hashOut: String, tag: String): Unit = {
    addOutput(nodeId, hashOut, tag, null)
  }

  override def addOutput(nodeId: String, hashOut: String, tag: String, value: String): Unit = {

  }

  override def reset(tag: String): Unit = {

  }
}
