package org.apache.flink.streaming.scala.examples.functions.sink

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.configuration.Configuration
import java.net.Socket
import java.io.PrintStream

import org.slf4j.LoggerFactory

class SocketSinkFunction[Any](host: String, port: Int) extends RichSinkFunction[Any] {

    protected var out: PrintStream = _
    protected var socket: Socket = _

    private val logger = LoggerFactory.getLogger(classOf[SocketSinkFunction[Any]])

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
        logger.info(s"Connecting to socket server: $host : $port")
        socket = new Socket(host, port)
        out = new PrintStream(socket.getOutputStream)
        logger.info("Done connecting")
    }

    @throws[Exception]
    override def close(): Unit = {
        socket.close()
    }

    @throws[Exception]
    override def invoke(input: Any): Unit = {
        out.println(input.toString)
    }
}
