package org.mlpiper.output

import java.net.SocketTimeoutException
import java.util.concurrent.{LinkedBlockingDeque, TimeUnit}
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import org.slf4j.LoggerFactory

/**
  * Events socket sending infrastructure
  * for Flink Streaming EventSocketSink
  *
  * (used in HealthEventStreamSocketSink)
  *
  * @param host        source host
  * @param port        source port
  * @param sharedQueue queue of messages to send
  */
class HealthEventSocketClientThread(host: String, port: Int, sharedQueue: LinkedBlockingDeque[ReflexEvent], writeMode: Boolean) extends Runnable {
  private val logger = LoggerFactory.getLogger(classOf[HealthEventSocketClientThread])
  private val client = new HealthEventSocket(host, port)
  private var stopCbCalled = false
  private val timeoutMSEC = 1000


  override def run(): Unit = {
    var obj: ReflexEvent = null
    var objWritten = false
    var runSocket = true

    logger.debug("Running health event socket client thread")
    while (runSocket) {
      try {
        if (!client.isConnected()) {
          client.initSocket()
        }

        if (client.isConnected()) {
          if (writeMode) {
            objWritten = false
            obj = sharedQueue.poll(timeoutMSEC, TimeUnit.MILLISECONDS)
            if (obj != null) {
              obj.writeDelimitedTo(client.getOutputStream)
              logger.debug(s"Sending ReflexEvent: type: ${obj.eventType}")
              //object written into socket : no need to enqueue in case of errors
              objWritten = true
            }
          } else {
            // read mode
            val obj = ReflexEvent.parseDelimitedFrom(client.getInputStream)
            if (obj.isDefined) {
              logger.debug(s"Received ReflexEvent: type: ${obj.get.eventType}")
              sharedQueue.put(obj.get)
            } else {
              client.closeSocket()
            }
          }
        }
      } catch {
        case e: SocketTimeoutException =>
          Thread.sleep(timeoutMSEC)
        case e@(_: java.net.ConnectException | _: java.net.SocketException | _: java.io.EOFException) =>
          client.closeSocket()
          Thread.sleep(timeoutMSEC)
        case e: Exception =>
          logger.warn("Uncaught exception: ", e.toString)
      } finally {
        if (writeMode && objWritten == false && obj != null) {
          // put back the data and try again
          sharedQueue.addFirst(obj)
        }
        if (stopCbCalled) {
          if (writeMode) {
            if (sharedQueue.isEmpty) {
              runSocket = false
            }
          } else {
            runSocket = false
          }
        }
      }
    }
    logger.debug("Done running, closing socket")
    client.closeSocket()
  }

  def stop(): Unit = {
    logger.debug("Stopping health event socket client thread")
    stopCbCalled = true
  }
}
