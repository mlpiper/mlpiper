/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.scala.examples.common.parameters.io.kafka

import org.apache.flink.streaming.scala.examples.common.parameters.io.kafka.common.{KafkaConfig, KafkaHostname, KafkaPort, KafkaTopic}

case object KafkaConsumerHostName extends KafkaHostname {
  override lazy val key = "kafkaConsumerHostname"
  override val label: String = "Kafka Hostname"
  override val kafkaPort = KafkaConsumerPort
  override val kafkaTopic = KafkaConsumerTopic
}

case object KafkaConsumerTopic extends KafkaTopic {
  override lazy val key = "kafkaConsumerTopic"
  override val label: String = "Kafka Topic"
}

case object KafkaConsumerPort extends KafkaPort {
  override val key = "kafkaConsumerPort"
  override val label: String = "Kafka Port"
}

case object KafkaConsumerConfig extends KafkaConfig {
  override val key = "kafkaConsumerConfigPath"
  override val label: String = "Kafka Config Path"
  override val kafkaTopic: KafkaTopic = KafkaConsumerTopic
}

