package com.parallelmachines.reflex.web

import java.net.BindException

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import com.parallelmachines.reflex.web.handler.{CommandsHandler, StatisticsHandler}
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{Channel, ChannelInitializer, ChannelOption}
import io.netty.handler.codec.http.HttpServerCodec
import io.netty.handler.logging.LoggingHandler
import org.slf4j.LoggerFactory


class RestServer(val port : Int) extends ChannelInitializer[Channel] with Runnable  {
  private val logger = LoggerFactory.getLogger(getClass)
  val eventLoopGroup = new NioEventLoopGroup()
  var webThread = new Thread(this)


  def start() : Unit = {
    webThread.start()
  }

  def stop() : Unit = {
    eventLoopGroup.shutdownGracefully()
  }


  def waitForExit() : Unit = {
      logger.info("Wait for exit command ....")
      webThread.join(30*1000)
      logger.info("Rest server exited")
      if (webThread.isAlive()) {
        logger.info("Thread is still alive after timeout - calling stop")
        this.stop()
        logger.info("After callign stop, waiting for thread to exit")
        webThread.join()
      }
  }

  @throws[Exception]
  override protected def initChannel(ch: Channel): Unit = {
    ch.pipeline.addLast("logger", new LoggingHandler())
    ch.pipeline.addLast(new HttpServerCodec())
    ch.pipeline.addLast(new StatisticsHandler())
    ch.pipeline.addLast(new CommandsHandler())
  }

  override def run(): Unit = {

    val bootstrap = createServerBootstrap()
    val maxTryCount = 120
    val sleepTimeMs = 1000

    var bindOk = false
    try {
      logger.info(s"REST server listens on port ${port} ...")


      var serverChannel:Channel = null
      var i = 0
      while (!bindOk && i < maxTryCount) {
        logger.info(s"REST server Trying to bind: try_index $i")
        bindOk = false
        try {
          serverChannel = bootstrap.bind(port).sync().channel()
          bindOk = true
        } catch {
          case _: BindException =>
            logger.warn(s"Failed to bind try = $i")
            Thread.sleep(sleepTimeMs)
        }

        i += 1
      }

      if (!bindOk) {
          val err = "REST server reached max bind try count - unable to bind to port"
          logger.error(err)
          throw new Exception(err)
      }

      logger.info(s"REST server bind succeeded")
      serverChannel.closeFuture().sync()
      logger.info(s"Server channel was released")
    } finally {
      logger.info("Exiting REST server thread ...")
      eventLoopGroup.shutdownGracefully()
    }
  }

  private def createServerBootstrap(): ServerBootstrap = {
    new ServerBootstrap()
      .channel(classOf[NioServerSocketChannel])
      .group(eventLoopGroup)
      .option[java.lang.Boolean](ChannelOption.SO_REUSEADDR, true)
      .childOption[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
      .childOption[java.lang.Boolean](ChannelOption.SO_REUSEADDR, true)
      .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      .childHandler(this)
  }
}

