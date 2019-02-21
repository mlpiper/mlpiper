package org.apache.flink.streaming.scala.examples.common.serialize

import java.io.FileOutputStream

import org.apache.flink.api.common.io.{OutputFormat, RichOutputFormat}
import org.apache.flink.configuration.Configuration

/** *
  * A OutputFormat to serialize and write batch training models
  *
  * @param path path to write the serialized object
  * @tparam T type of object
  */
class BatchModelOutputFormat[T](path: String) extends OutputFormat[T] {

  var file: FileOutputStream = _

  override def configure(param: Configuration): Unit = {
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    require(numTasks == 1, "Parallelism has to be set to 1")
    file = new FileOutputStream(path)
  }

  override def writeRecord(record: T) = {
    file.write(record.toString.map(_.toByte).toArray)
  }

  override def close() = {
    file.close()
  }
}
