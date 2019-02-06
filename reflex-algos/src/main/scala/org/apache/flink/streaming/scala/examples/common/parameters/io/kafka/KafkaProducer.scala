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

case object KafkaProducerHostName extends KafkaHostname {
  override lazy val key = "kafkaProducerHostname"
  override val label = "Kafka Hostname"
  override val kafkaPort = KafkaProducerPort
  override val kafkaTopic = KafkaProducerTopic
}

case object KafkaProducerTopic extends KafkaTopic {
  override lazy val key = "kafkaProducerTopic"
  override val label = "Kafka Topic"
}

case object KafkaProducerPort extends KafkaPort {
  override val key = "kafkaProducerPort"
  override val label = "Kafka Port"
}

case object KafkaProducerConfig extends KafkaConfig {
  override val key = "kafkaProducerConfigPath"
  override val label = "Kafka Config Path"
  override val kafkaTopic: KafkaTopic = KafkaProducerTopic
}
