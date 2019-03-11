package org.mlpiper.output

import java.util.concurrent.{LinkedBlockingDeque, TimeUnit}

import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import org.slf4j.LoggerFactory

/**
  * Used in:
  * - Flink Batch EventSocketSink
  * - Spark Batch EventSocketSink
  */
object SocketSinkSingleton {
  val sharedQueue = new LinkedBlockingDeque[ReflexEvent]()
  var socketObject: HealthEventSocketClientThread = _
  var socketThread: Thread = _
  /**
    * Counter field is required because startClient()
    * will be called each time when output format is registered for DataSet.
    * (in Flink batch EventSocketSink)
    */
  var counter = 0
  private val logger = LoggerFactory.getLogger(getClass)

  def startClient(host: String, port: Int, writeMode: Boolean): Unit = {
    this.synchronized {
      if (counter == 0) {
        logger.debug("Starting socket sink client")
        socketObject = new HealthEventSocketClientThread(host, port, sharedQueue, writeMode)
        socketThread = new Thread(socketObject)
        socketThread.start()
      }
      counter += 1
    }
  }

  def stopClient(): Unit = {
    this.synchronized {
      counter -= 1
      if (counter == 0) {
        logger.debug("Stopping socket sink client")
        socketObject.stop()
        socketThread.join()
        logger.debug("After join - thread closed")
      }
    }
  }

  def putRecord(record: ReflexEvent): Unit = {
    sharedQueue.put(record)
  }

  def getRecord(): ReflexEvent = {
    val timeoutMSEC = 1000
    sharedQueue.poll(timeoutMSEC, TimeUnit.MILLISECONDS)
  }
}
