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
package org.apache.flink.streaming.scala.examples.functions.io.kafka

import java.util.Properties

import org.slf4j.LoggerFactory

object PMKafka {

  private[kafka] val LOG = LoggerFactory.getLogger(this.getClass)

  /**
    * Creates the most basic properties file to instantiate a Kafka consumer.
    * @param hostName Host name of the Kafka consumer.
    * @param kafkaPort Port of the Kafka consumer.
    * @return Kafka properties as a [[java.util.Properties]].
    */
  def createBasicProperties(hostName: String,
                            kafkaPort: Int)
  : Properties = {
    require(hostName != null, "Host name cannot be null")
    require(kafkaPort >= 0, "Kafka port must be positive")

    val props: Properties = new Properties
    props.setProperty("bootstrap.servers", hostName + ":" + kafkaPort.toString)
    props.setProperty("group.id", "text")
    props
  }

}

