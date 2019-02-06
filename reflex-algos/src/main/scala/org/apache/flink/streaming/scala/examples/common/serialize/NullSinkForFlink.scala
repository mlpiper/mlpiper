package org.apache.flink.streaming.scala.examples.common.serialize

import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.configuration.Configuration

class NullSinkForFlink[T] extends RichOutputFormat[T] {
  override def configure(parameters: Configuration) = {
    /*Do Nothing*/
  }

  override def writeRecord(record: T) = {
    /*Do Nothing*/
  }

  override def close() = {
    /*Do Nothing*/
  }

  override def open(taskNumber: Int, numTasks: Int) = {
    /*Do Nothing */
  }
}
