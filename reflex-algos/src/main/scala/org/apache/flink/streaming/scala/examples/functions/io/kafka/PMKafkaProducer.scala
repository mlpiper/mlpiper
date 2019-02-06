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

import breeze.linalg.{DenseVector => BreezeDenseVector}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.streaming.scala.examples.clustering.math.LabeledVector
import org.apache.flink.streaming.scala.examples.clustering.utils.PropertiesWrapper
import org.apache.flink.streaming.scala.examples.common.parameters.io.kafka._
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterTool
import org.apache.flink.streaming.scala.examples.functions.conversion.{BreezeVectorToStringFlatMap, LabeledVectorToStringFlatMap}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.slf4j.LoggerFactory

object PMKafkaProducer {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def fromArgumentParameters(env: StreamExecutionEnvironment,
                             params: ArgumentParameterTool)
  : PMKafkaProducer = {
    val kafkaTopic = params.getString(KafkaProducerTopic)

    if (params.containsNonEmpty(KafkaProducerConfig)) {
      if (params.containsUserSpecified(KafkaProducerHostName) ||
        params.containsUserSpecified(KafkaProducerPort)) {
        LOG.info(KafkaProducerConfig.key + " provided, ignoring "
          + KafkaProducerHostName.key + " and " + KafkaProducerPort.key)
      }

      val properties = PropertiesWrapper.toProperties(params.getString(KafkaProducerConfig))
      require(properties.isDefined, "Invalid Kafka Producer properties")

      new PMKafkaProducer(env, kafkaTopic, properties.get)
    } else {
      new PMKafkaProducer(env,
        params.getString(KafkaProducerHostName), params.getInt(KafkaProducerPort), kafkaTopic)
    }
  }
}


/**
  * Creates a Kafka producer using the provided topic and properties file.
  * @param env StreamExecutionEnvironment of the job.
  * @param topicName Topic name of the Kafka producer.
  * @param kafkaProducerConfig Kafka properties as a [[java.util.Properties]].
  */
class PMKafkaProducer(env: StreamExecutionEnvironment,
                      topicName: String,
                      kafkaProducerConfig: Properties) {

  /**
    * Creates the most basic Kafka producer.
    * @param env StreamExecutionEnvironment of the job.
    * @param hostName Host name of the Kafka producer.
    * @param kafkaPort Port of the Kafka producer.
    * @param topicName Topic name of the Kafka producer.
    */
  def this(env: StreamExecutionEnvironment,
           hostName: String,
           kafkaPort: Int,
           topicName: String) = {
    this(env, topicName, PMKafka.createBasicProperties(hostName, kafkaPort))
  }

  private lazy val kafkaProducerSink = new FlinkKafkaProducer09[String](
    topicName, new SimpleStringSchema, kafkaProducerConfig)

  def fromVectorStream(vectorStream: DataStream[BreezeDenseVector[Double]],
                       elementSeparator: Char,
                       debug: Boolean = false)
  : Unit = {
    asScalaStream(vectorStream)
      .flatMap(new BreezeVectorToStringFlatMap(elementSeparator, debug))
      .addSink(kafkaProducerSink)
  }

  def fromLabeledVectorStream(labeledVectorStream: DataStream[LabeledVector[Double]],
                              elementSeparator: Char,
                              labelSeparator: Option[Char] = None,
                              timestampSeparator: Option[Char] = None)
  : Unit = {
    labeledVectorStream
      .flatMap(new LabeledVectorToStringFlatMap(
        elementSeparator, labelSeparator, timestampSeparator))
      .addSink(kafkaProducerSink)
  }

  def fromStringStream(stringStream: DataStream[String])
  : Unit = {
    stringStream.addSink(kafkaProducerSink)
  }
}
