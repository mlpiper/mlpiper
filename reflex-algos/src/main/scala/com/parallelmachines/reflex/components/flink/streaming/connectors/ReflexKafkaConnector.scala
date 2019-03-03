package com.parallelmachines.reflex.components.flink.streaming.connectors

import java.util.Properties

import com.parallelmachines.reflex.components.flink.streaming.{FlinkStreamingComponent, StreamExecutionEnvironment}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._


class ReflexKafkaConnector extends FlinkStreamingComponent {
  val isSource = true

  var kafkaHostArg = "host"
  var kafkaPortArg = "port"
  var kafkaTopicArg = "topic"

  var kafkaHost: String = "localhost"
  var kafkaPort: Int = 9092
  var kafkaTopic: String = "reflex"

  override val group: String = ComponentsGroups.connectors
  override val label: String = "Kafka"
  override val description: String = "Get data from Kafka"
  override val version: String = "1.0.0"
  override lazy val paramInfo: String =
    s"""[
        {"key": "$kafkaHostArg", "type": "string", "${JsonHeaders.LabelHeader}":"Kafka Host", "${JsonHeaders.DescriptionHeader}": "Kafka host to connect to"},
        {"key": "$kafkaPortArg", "type": "int", "${JsonHeaders.LabelHeader}":"Kafka Port", "${JsonHeaders.DescriptionHeader}": "Kafka port number to connect to"},
        {"key": "$kafkaTopicArg", "type": "string", "${JsonHeaders.LabelHeader}":"Topic", "${JsonHeaders.DescriptionHeader}": "Kafka topic name"}
        ]
    """.stripMargin

  override val inputTypes: ConnectionList = ConnectionList.empty()

  val output = ComponentConnection(
    tag = typeTag[String],
    label = "String",
    description = "Data received from a topic",
    group = ConnectionGroups.DATA)

  override var outputTypes: ConnectionList = ConnectionList(output)

  override def configure(paramMap: Map[String, Any]): Unit = {
    if (paramMap.contains(kafkaHostArg)) {
      kafkaHost = paramMap(kafkaHostArg).asInstanceOf[String]
    }
    if (paramMap.contains(kafkaPortArg)) {
      kafkaPort = paramMap(kafkaPortArg).asInstanceOf[BigInt].toInt
    }
    if (paramMap.contains(kafkaTopicArg)) {
      kafkaTopic = paramMap(kafkaTopicArg).asInstanceOf[String]
    }
  }

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    ArrayBuffer(new DataWrapper())
  }
}
