package com.parallelmachines.reflex.test.reflexpipeline

import java.net._

import com.google.protobuf.InvalidProtocolBufferException
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import scala.collection.mutable


class HealthEventWriteReadSocketServer(inputItems: List[ReflexEvent], writeMode: Boolean = true, var waitExplicitStop: Boolean = false) {
  val server: ServerSocket = new ServerSocket(0)
  val port: Int = server.getLocalPort
  val outputList = new mutable.MutableList[ReflexEvent]

  def run(): Unit = {
    var sock: Socket = null
    do {
      var isConnected = false

      try {
        sock = server.accept()
        sock.setSoTimeout(100)
        isConnected = true
      } catch {
        case _: java.net.SocketException =>
          isConnected = false
      }

      if (writeMode) {
        for (item <- inputItems) {
          item.writeDelimitedTo(sock.getOutputStream)
        }
      } else {
        while (isConnected) {
          try {
            val out = ReflexEvent.parseDelimitedFrom(sock.getInputStream)
            if (out.isDefined) {
              val inData = out.get
              outputList += inData
            } else {
              isConnected = false
            }
          } catch {
            case _ | _: SocketTimeoutException =>
            case e: InvalidProtocolBufferException =>
              isConnected = false
              throw e
            case e: java.io.EOFException =>
              isConnected = false
              throw e
          }
        }
      }
      sock.close()
    } while (waitExplicitStop)
  }

  def forceStop(): Unit = {
    waitExplicitStop = false
  }
}

