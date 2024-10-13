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

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.internal.Logging
import org.apache.spark.lineage.dto.{LFlow, LNodeLink, LNodeRegistration}

class LineageDispatcher extends Logging {

  val kafkaConfig = new java.util.HashMap[String, Object]()
  // Local Execution
  // kafkaConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
  //  "localhost:29092,localhost:39092,localhost:49092")
  kafkaConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
   "kafka-1:29092,kafka-2:39092,kafka-3:49092")
  // Increase the buffer to handle thirty partitions and generally large volume of data
  kafkaConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "134217728")
  // Limit the size of a single request to 2 MB
  kafkaConfig.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "2150400")
  // Batch up to 500 KB before sending to the server
  kafkaConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, "512000")
  // Give the producer 100ms to collect records into the batch
  kafkaConfig.put(ProducerConfig.LINGER_MS_CONFIG, "100")

  // ack the messages to ensure at-least-once semantics
  kafkaConfig.put(ProducerConfig.ACKS_CONFIG, 1)
  // prevent deduplication of messages
  kafkaConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
  // retry - wait for 500ms and try three times
  kafkaConfig.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "500")
  // retry - ensure that messages remain in order
  kafkaConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
  kafkaConfig.put(ProducerConfig.RETRIES_CONFIG, "3")

  private val producer = new KafkaProducer(kafkaConfig,
    new StringSerializer(), new StringSerializer())
  private val jsonMapper: JsonMapper = JsonMapper
    .builder()
    .addModule(DefaultScalaModule)
    .addModule(new JavaTimeModule())
    .serializationInclusion(JsonInclude.Include.NON_NULL)
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
    val record = new ProducerRecord[String, String]("lineage-flow", messageKey, flow.toCsvString())
    producer.send(record)
  }

  def open(): Unit = {
    log.warn("Open connection to kafka")
  }

  def close(): Unit = {
    log.warn("Close connection to kafka")
    producer.close()
  }

}

object LineageDispatcher {
  private[spark] val instance: LineageDispatcher = new LineageDispatcher()

  def stop(): Unit = {
    instance.close()
  }

  def start(): Unit = {
    instance.open()
  }

  def getInstance: LineageDispatcher = {
    return instance
  }
}