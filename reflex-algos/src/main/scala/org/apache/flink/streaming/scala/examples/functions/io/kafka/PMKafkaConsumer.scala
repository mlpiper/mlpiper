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
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.scala.examples.clustering.math.LabeledVector
import org.apache.flink.streaming.scala.examples.clustering.utils.PropertiesWrapper
import org.apache.flink.streaming.scala.examples.common.parameters.io.kafka.{KafkaConsumerConfig, KafkaConsumerHostName, KafkaConsumerPort, KafkaConsumerTopic}
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterTool
import org.apache.flink.streaming.scala.examples.common.parsing.ParameterIndices
import org.apache.flink.streaming.scala.examples.functions.conversion._
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SimpleStringSchema}
import org.slf4j.LoggerFactory


object PMKafkaConsumer {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def fromArgumentParameters(env: StreamExecutionEnvironment,
                             params: ArgumentParameterTool)
  : PMKafkaConsumer = {
    val kafkaTopic = params.getString(KafkaConsumerTopic)

    if (params.containsNonEmpty(KafkaConsumerConfig)) {
      if (params.containsUserSpecified(KafkaConsumerHostName) ||
        params.containsUserSpecified(KafkaConsumerPort)) {
        LOG.info(KafkaConsumerConfig.key + " provided, ignoring "
          + KafkaConsumerHostName.key + " and " + KafkaConsumerPort.key)
      }

      val properties = PropertiesWrapper.toProperties(params.getString(KafkaConsumerConfig))
      require(properties.isDefined, "Invalid Kafka Consumer properties")

      new PMKafkaConsumer(env, kafkaTopic, properties.get)
    } else {
      new PMKafkaConsumer(env,
        params.getString(KafkaConsumerHostName), params.getInt(KafkaConsumerPort), kafkaTopic)
    }
  }

}

/**
  * Creates a Kafka consumer using the provided topic and properties file.
  * @param env StreamExecutionEnvironment of the job.
  * @param topicName Topic name of the Kafka consumer.
  * @param kafkaConsumerConfig Kafka properties as a [[java.util.Properties]].
  */
class PMKafkaConsumer(env: StreamExecutionEnvironment,
                      topicName: String,
                      kafkaConsumerConfig: Properties,
                      isTestMode: Boolean = false) {

  /**
    * Creates the most basic Kafka consumer.
    * @param env StreamExecutionEnvironment of the job.
    * @param hostName Host name of the Kafka consumer.
    * @param kafkaPort Port of the Kafka consumer.
    * @param topicName Topic name of the Kafka consumer.
    */
  def this(env: StreamExecutionEnvironment,
           hostName: String,
           kafkaPort: Int,
           topicName: String) = {
    this(env, topicName, PMKafka.createBasicProperties(hostName, kafkaPort))
  }

  @deprecated
  private class PMKafkaConsumer09[Type] (topic:String,
                                        valueDeserializer:DeserializationSchema[Type],
                                        props: Properties)
    extends FlinkKafkaConsumer09[Type](topic, valueDeserializer, props) {
    final def stop(): Unit = {
      cancel()
    }
  }

  /** DataStream of text from the Kafka consumer. */
  private val textStream: DataStream[String] = env.addSource(
    if (isTestMode) {
      new PMKafkaConsumer09[String](topicName, new SimpleStringSchema, kafkaConsumerConfig)
    } else {
      new FlinkKafkaConsumer09[String](topicName, new SimpleStringSchema, kafkaConsumerConfig)
    })


  def toVectorStream(vectorLength: Int,
                     elementSeparator: Char,
                     debug: Boolean = false): DataStream[BreezeDenseVector[Double]] = {
    textStream.flatMap(new StringToBreezeVectorFlatMap(vectorLength, elementSeparator, debug))
  }

  def toLabeledVectorStream(vectorLength: Int,
                            elementSeparator: Char,
                            labelSeparator: Option[Char] = None,
                            timestampSeparator: Option[Char] = None,
                            debug: Boolean = false): DataStream[LabeledVector[Double]] = {
    textStream.flatMap(new StringToLabeledVectorFlatMap(
      vectorLength,
      elementSeparator,
      labelSeparator,
      timestampSeparator,
      debug))
  }

  def toIndexedLabeledVectorStream(nrInputElements: Int,
                            elementSeparatorRegex: String,
                            indices: ParameterIndices,
                            labelIndex: Option[Int] = None,
                            timestampIndex: Option[Int] = None): DataStream[LabeledVector[Double]] = {
    textStream.flatMap(new StringToIndexedLabeledVectorFlatMap(
      nrInputElements, elementSeparatorRegex, indices, labelIndex, timestampIndex))
  }
}

