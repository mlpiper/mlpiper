package com.parallelmachines.reflex.components.flink.batch.functions

import com.parallelmachines.reflex.output.SocketSinkSingleton
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent

/** *
  * A Flink OutputFormat to serialize and write batch training models
  *
  * @param host to connect to
  * @param port to connect to
  */

class EventSocketSinkBatchOutputFormat(host: String, port: Int) extends OutputFormat[ReflexEvent] {

  override def configure(param: Configuration): Unit = {
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    SocketSinkSingleton.startClient(host, port, writeMode = true)
  }

  override def writeRecord(record: ReflexEvent) = {
    SocketSinkSingleton.putRecord(record)
  }

  override def close() = {
    SocketSinkSingleton.stopClient()
  }
}