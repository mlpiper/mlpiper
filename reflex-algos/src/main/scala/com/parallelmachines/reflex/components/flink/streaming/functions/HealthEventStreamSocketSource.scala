package com.parallelmachines.reflex.components.flink.streaming.functions

import java.net.SocketTimeoutException

import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.output.HealthEventSocket
import org.apache.flink.api.common.functions.StoppableFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.slf4j.LoggerFactory

import scala.util.control.Breaks._

/**
  * Event socket receiving infrastructure
  * for Flink Streaming EventSocketSource
  *
  * @param host source host
  * @param port source port
  */
class HealthEventStreamSocketSource(host: String, port: Int)
  extends RichParallelSourceFunction[ReflexEvent]()
    with StoppableFunction {

  private val logger = LoggerFactory.getLogger(classOf[HealthEventStreamSocketSource])
  private val client = new HealthEventSocket(host, port)
  private var stopFlag = false
  private val timeoutMSEC = 1000

  override def run(ctx: SourceFunction.SourceContext[ReflexEvent]) {
    breakable {
      while (!stopFlag) {
        try {
          if (!client.isConnected) {
            client.initSocket()
          }

          if (client.isConnected()) {
            val obj = ReflexEvent.parseDelimitedFrom(client.getInputStream)
            if (obj.isDefined) {
              logger.debug(s"Received ReflexEvent: ${obj.get}")
              ctx.collect(obj.get)
            } else {
              // when object is null, means connection was closed
              client.closeSocket()
            }
          }
        }
        catch {
          case e: SocketTimeoutException =>
            Thread.sleep(timeoutMSEC)
          case e@(_: java.net.ConnectException | _: java.net.SocketException | _: java.io.EOFException) =>
            client.closeSocket()
            Thread.sleep(timeoutMSEC)
          case e: Exception =>
            logger.warn("Uncaught exception: ", e.toString)
        }
      }
    }
    ctx.close()
  }

  override def open(parameters: Configuration): Unit = {
  }

  override def close(): Unit = {
    client.closeSocket()
  }

  override protected def stop() = {
    stopFlag = true
  }

  override def cancel(): Unit = {
    stopFlag = true
  }
}