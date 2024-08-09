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

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import org.apache.spark.lineage.config.ProducerConfig.getConfig
import org.apache.spark.lineage.dto.{LFlow, LNodeLink, LNodeRegistration}

class LineageDispatcher {

  private val config = getConfig("./kafka.conf")
  private val producer = new KafkaProducer(config, new StringSerializer(), new StringSerializer())
  private val jsonMapper: JsonMapper = JsonMapper
    .builder()
    .addModule(DefaultScalaModule)
    .addModule(new JavaTimeModule())
    .build()

  def register(nodeRegistration: LNodeRegistration): Unit = {
    val record = new ProducerRecord[String, String]("lineage-node-registration",
      "lineage-node-registration", jsonMapper.writeValueAsString(nodeRegistration))
    producer.send(record)
  }

  def link(nodeLink: LNodeLink): Unit = {
    val record = new ProducerRecord[String, String]("lineage-node-link", "lineage-node-link",
      jsonMapper.writeValueAsString(nodeLink))
    producer.send(record)
  }

  def capture(key: String, flow: LFlow): Unit = {
    val record = new ProducerRecord[String, String]("lineage-flow", key,
      jsonMapper.writeValueAsString(flow))
    producer.send(record)
  }
}
