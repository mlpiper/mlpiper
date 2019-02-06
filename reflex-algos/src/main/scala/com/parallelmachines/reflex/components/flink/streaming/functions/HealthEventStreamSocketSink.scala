package com.parallelmachines.reflex.components.flink.streaming.functions

import java.util.concurrent.LinkedBlockingDeque

import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.output.HealthEventSocketClientThread
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * Infra for sending events through sockets
  * for Flink Streaming EventSocketSink
  *
  * @param host source host
  * @param port source port
  */
class HealthEventStreamSocketSink(host: String, port: Int) extends RichSinkFunction[ReflexEvent]() {

  val sharedQueue = new LinkedBlockingDeque[ReflexEvent]()
  var socketObject: HealthEventSocketClientThread = _
  var socketThread: Thread = _

  /**
    * Counter field is required because startClient()
    * will be called each time when output format is registered for DataSet.
    * (in Flink batch EventSocketSink)
    */
  private var counter = 0

  override def open(parameters: Configuration): Unit = {
    this.synchronized {
      if (counter == 0) {
        socketObject = new HealthEventSocketClientThread(host, port, sharedQueue, writeMode = true)
        socketThread = new Thread(socketObject)
        socketThread.start()
      }
      counter += 1
    }
  }

  override def close(): Unit = {
    this.synchronized {
      counter -= 1
      if (counter == 0) {
        socketObject.stop()
        socketThread.join()
      }
    }
  }

  override def invoke(input: ReflexEvent): Unit = {
    sharedQueue.put(input)
  }
}
