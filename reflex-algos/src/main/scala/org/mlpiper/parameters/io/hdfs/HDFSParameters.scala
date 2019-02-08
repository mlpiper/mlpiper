package org.mlpiper.parameters.io.hdfs

import org.mlpiper.parameters.common.{ArgumentParameterChecker, ArgumentParameterTool, ExistingFilePathParameter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.{FileSystem, Path}

case object HDFSHostname extends Hostname {
  override lazy val key: String = "host"
  override val label: String = "HDFS host"
  override val required = true
  override val description: String = "HDFS host"
  override lazy val errorMessage: String = key + " must be a String. Must have " +
    HDFSPort.key + " parameter set."

  override def condition(hostname: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    super.condition(hostname, parameters) &&
      parameters.containsNonEmpty(HDFSPort)
  }
}

case object HDFSPort extends Port {
  override lazy val key: String = "port"
  override val label: String = "HDFS port"
  override val required: Boolean = true
  override val description: String = "HDFS port"
  override lazy val errorMessage: String = key + " must be Int. Must have " +
    HDFSHostname.key + " parameter set."

  override def condition(port: Option[Int],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    super.condition(port, parameters) &&
      parameters.containsNonEmpty(HDFSHostname)
  }
}

case object HDFSFilePath extends ExistingFilePathParameter {
  override val key: String = "fileName"
  override val label: String = "HDFS file name"
  override val required: Boolean = false
  override val description: String = "File path to use as input"

  override def condition(filePath: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    if (!(parameters.containsNonEmpty(HDFSHostname) &&
      parameters.containsNonEmpty(HDFSPort) &&
      filePath.isDefined)) {
      return false
    }

    val params = parameters.asInstanceOf[ArgumentParameterTool]
    val hadoopConf = new Configuration
    hadoopConf.set("fs.defaultFS", s"hdfs://${params.get(HDFSHostname)}:${params.get(HDFSPort)}")
    val fs = FileSystem.get(hadoopConf)
    val path = new Path(filePath.get)
    fs.exists(path) &&
      fs.isFile(path) &&
      fs.getFileStatus(path).getPermission.getUserAction.implies(FsAction.READ)
  }
}
