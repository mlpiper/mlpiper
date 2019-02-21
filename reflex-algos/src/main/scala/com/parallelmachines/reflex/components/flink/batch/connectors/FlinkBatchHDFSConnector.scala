package com.parallelmachines.reflex.components.flink.batch.connectors

import org.apache.flink.streaming.scala.examples.common.parameters.io.hdfs.{HDFSFilePath, HDFSHostname, HDFSPort}
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterTool

class FlinkBatchHDFSConnector extends FlinkBatchFileConnector {
  override val label = "HDFS"
  override val description = "Reads text file from HDFS"
  override val version = "1.0.0"

  var params = new ArgumentParameterTool("HDFS Connector")
  params.add(HDFSHostname)
  params.add(HDFSPort)
  params.add(HDFSFilePath)

  override lazy val paramInfo = s"""[${params.toJson()}]"""

  override def configure(paramMap: Map[String, Any]): Unit = {
    params.initializeParameters(paramMap)
    fileName = s"hdfs://${params.get(HDFSHostname)}:${params.get(HDFSPort).toString}${params.get(HDFSFilePath)}"
  }
}
