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

import java.util.Properties

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringSerializer

import org.apache.spark.lineage.dto.{LFlow, LNodeLink, LNodeRegistration}

class LineageDispatcher(clientId: String) {

  private val producer = new KafkaProducer(getProducerConfig,
    new StringSerializer(), new StringSerializer())
  private val jsonMapper: JsonMapper = JsonMapper
    .builder()
    .addModule(DefaultScalaModule)
    .addModule(new JavaTimeModule())
    .build()

  def register(messageKey: String, nodeRegistration: LNodeRegistration): Unit = {
    val headers: Header = new RecordHeader("type", "LineageNodeRegistration".getBytes("UTF-8"))
    val record = new ProducerRecord[String, String]("lineage-node", null, messageKey,
      jsonMapper.writeValueAsString(nodeRegistration),
      java.util.Collections.singletonList(headers))
    producer.send(record)
  }

  def link(messageKey: String, nodeLink: LNodeLink): Unit = {
    val headers: Header = new RecordHeader("type", "LineageNodeLink".getBytes("UTF-8"))
    val record = new ProducerRecord[String, String]("lineage-node", null, messageKey,
      jsonMapper.writeValueAsString(nodeLink),
      java.util.Collections.singletonList(headers))
    producer.send(record)
  }

  def capture(messageKey: String, flow: LFlow): Unit = {
    val record = new ProducerRecord[String, String]("lineage-flow", messageKey,
      jsonMapper.writeValueAsString(flow))
    producer.send(record)
  }

  def close(): Unit = {
    producer.close()
  }

  private def getProducerConfig: Properties = {
    val props = new Properties()
    val configFile = getClass.getResourceAsStream("/kafka.properties")

    props.load(configFile)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId)
    props
  }
}
