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
package org.apache.flink.streaming.scala.examples.common.parameters.io.kafka.common

import org.apache.flink.streaming.scala.examples.common.parameters.io.common.Hostname
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterChecker

trait KafkaHostname extends Hostname {
  override val required = false
  val kafkaTopic: KafkaTopic
  val kafkaPort: KafkaPort

  override val description: String = "Kafka hostname"
  override lazy val errorMessage: String = key + " must be a String. Must have " +
    kafkaTopic.key + " and " + kafkaPort.key + " parameters set."

  override def condition(hostname: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    super.condition(hostname, parameters) &&
      parameters.containsNonEmpty(kafkaTopic) &&
      parameters.containsNonEmpty(kafkaPort)
  }
}
