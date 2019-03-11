package org.mlpiper.output

import java.io.{InputStream, OutputStream}
import java.net.Socket

import org.slf4j.LoggerFactory

/**
  * Socket wrapping infrastructure.
  *
  * @param host socket host
  * @param port socket port
  */
class HealthEventSocket(host: String, port: Int) extends Serializable {

  private val logger = LoggerFactory.getLogger(classOf[HealthEventSocket])
  private var client: Socket = null
  private var connected = false
  private val timeoutMSEC = 1000
  private val hostname = if (host == null) "localhost" else host

  def initSocket(): Unit = {

    try {
      logger.info(s"Connecting to socket server: $hostname : $port")
      client = new Socket(host, port)
      client.setSoTimeout(timeoutMSEC)
      logger.info(s"Connected to $hostname : $port")
      connected = true
    } catch {
      case e: Throwable =>
        logger.info("Socket error: ", e.toString)
        closeSocket()
        throw e
    }
  }

  def closeSocket(): Unit = {
    if (client != null) {
      client.close()
      client = null
      connected = false
      logger.info(s"Connection closed $hostname : $port")
    }
  }

  def isConnected(): Boolean = {
    connected
  }

  def getInputStream(): InputStream = {
    client.getInputStream
  }

  def getOutputStream(): OutputStream = {
    client.getOutputStream
  }
}
