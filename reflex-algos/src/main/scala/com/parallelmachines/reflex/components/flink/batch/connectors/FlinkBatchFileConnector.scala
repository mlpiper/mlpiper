package com.parallelmachines.reflex.components.flink.batch.connectors

import java.io.File

import com.parallelmachines.reflex.components.flink.batch.FlinkBatchComponent
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer

class FlinkBatchFileConnector extends FlinkBatchComponent {

  var fileName: String = ""
  val fileNameArg: String = "fileName"
  val isSource = true

  val group: String = ComponentsGroups.connectors
  val label = "File"
  val description = "Reads text file"
  val version = "1.0.0"

  override lazy val paramInfo: String =
    s"""[
        {"${JsonHeaders.KeyHeader}":"$fileNameArg", "${JsonHeaders.TypeHeader}":"string", "${JsonHeaders.DescriptionHeader}" : "Specifies the file path to use as input"}
        ]""".stripMargin

  override val inputTypes: ConnectionList = ConnectionList.empty()

  val output = ComponentConnection(
    tag = typeTag[String],
    label = "Data",
    description = "Data read from a text file",
    group = ConnectionGroups.DATA)

  override var outputTypes: ConnectionList = ConnectionList(output)

  override def configure(paramMap: Map[String, Any]): Unit = {
    if (paramMap.contains(fileNameArg)) {
      fileName = paramMap(fileNameArg).asInstanceOf[String]
      require(new File(fileName).canRead, s"$fileName is not a readable file")
    } else {
      throw new Exception(s"Missing parameter $fileNameArg for Batch File Connector")
    }
  }

  override def materialize(env: ExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {

    val text = env.readTextFile(fileName)
    ArrayBuffer(DataWrapper(text))
  }
}
