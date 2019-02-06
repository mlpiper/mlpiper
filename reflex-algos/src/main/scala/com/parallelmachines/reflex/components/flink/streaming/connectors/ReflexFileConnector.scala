package com.parallelmachines.reflex.components.flink.streaming.connectors

import java.io.File

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class ReflexFileConnector extends FlinkStreamingComponent {
  var fileName: String = ""
  val fileNameArg: String = "fileName"

  override val isSource = true
  override val group: String = ComponentsGroups.connectors
  override val label = "File"
  override val description = "Reads text file"
  override val version = "1.0.0"
  override lazy val paramInfo: String =
    s"""[
          {"key":"$fileNameArg", "type":"string", "${JsonHeaders.LabelHeader}":"File Name", "${JsonHeaders.DescriptionHeader}" : "Specifies the file path to use as input"}
        ]""".stripMargin
  override val inputTypes: ConnectionList = ConnectionList.empty()

  val output = ComponentConnection(
    tag = typeTag[String],
    label = "String",
    description = "Data read from a text file",
    group = ConnectionGroups.DATA)

  override var outputTypes: ConnectionList = ConnectionList(output)

  override def configure(paramMap: Map[String, Any]): Unit = {
    if (paramMap.contains(fileNameArg)) {
      fileName = paramMap(fileNameArg).asInstanceOf[String]
      require(new File(fileName).canRead, s"$fileName is not a readable file")
    } else {
      throw new Exception(s"Missing parameter $fileNameArg for ReflexFileConnector")
    }
  }

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {

    val text = env.readTextFile(fileName)

    ArrayBuffer(new DataWrapper(text))
  }
}
