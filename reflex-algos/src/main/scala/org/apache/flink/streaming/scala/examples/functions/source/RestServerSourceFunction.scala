package org.apache.flink.streaming.scala.examples.functions.source

import java.security.MessageDigest

import com.parallelmachines.reflex.web.RestServer
import com.parallelmachines.reflex.web.handler.{RestAction, RestApi, RestCommands}
import io.netty.channel.Channel
import io.netty.handler.codec.http._
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import com.parallelmachines.reflex.web.helper.HandlerHelper
import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.util.CharsetUtil
import org.json4s._
import org.json4s.jackson.Json
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.collection.mutable


class RestServerSourceFunction(port: Int) extends RichParallelSourceFunction[Map[String, Any]] {
  var stop = false

  var server: RestServerSource = _

  override def run(ctx: SourceFunction.SourceContext[Map[String, Any]]): Unit = {
    server = new RestServerSource(ctx, port)
    server.start()

    while (server.waitForExit(millis = 5000)) {
      if (stop) {
        server.stop()
        server.waitForExit()
        return
      }
    }
  }

  override def cancel(): Unit = {
    stop = true
    server.waitForExit()
  }
}

class CommandsHandler(sourceCtx: SourceFunction.SourceContext[Map[String, Any]]) extends
  SimpleChannelInboundHandler[FullHttpRequest] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def channelRead0(ctx: ChannelHandlerContext, msg: FullHttpRequest): Unit = {
    msg.method match {
      case HttpMethod.POST =>
        val queryDecoder = new QueryStringDecoder(msg.uri())
        queryDecoder.path() match {
          case RestApi.Command =>
            val parameters = queryDecoder.parameters()
            if (parameters.containsKey(RestAction.Command)) {
              val cmd = parameters.get(RestAction.Command).get(0)
              cmd match {
                case RestCommands.Exit =>
                  logger.info("Exit received")
                  val replyJson =
                    s"""{
                       |"status" : "Stopping",
                       |}""".stripMargin
                  HandlerHelper.writePlainTextResponse(
                    Unpooled.copiedBuffer(replyJson, CharsetUtil.UTF_8), ctx)
                  closeChannels(ctx)
                case RestCommands.Run =>
                  val jsonStr = msg.content().toString(CharsetUtil.UTF_8)
                  logger.info(s"JSON from rest: [$jsonStr]")
                  val md5 = MessageDigest
                    .getInstance("MD5")
                    .digest(jsonStr.getBytes)
                    .map {
                      "%02x".format(_)
                    }.foldLeft("") {
                    _ + _
                  }

                  implicit val formats = org.json4s.DefaultFormats

                  var error: Boolean = false

                  try {
                    val map = parse(jsonStr).extract[Map[String, Any]]
                    sourceCtx.collect(map)
                  } catch {
                    case e: Exception =>
                      logger.error("Error while parsing json")
                      error = true
                  }

                  var statusStr = if (error) "Failed" else "OK"

                  val jsonMap = mutable.ListMap[String, Any]("status" -> statusStr, "md5" -> md5)
                  val jsonReplyStr = Json(DefaultFormats).write(jsonMap)
                  HandlerHelper.writePlainTextResponse(
                    Unpooled.copiedBuffer(jsonReplyStr, CharsetUtil.UTF_8), ctx)
                  closeRequest(ctx)
                case _ => HandlerHelper.writeResponseNotFound(ctx)
              }
            } else {
              HandlerHelper.writeResponseNotFound(ctx)
            }
          case _ => HandlerHelper.writeResponseNotFound(ctx)
        }
      case _ =>
        HandlerHelper.writeResponseNotFound(ctx)
    }
  }

  private def closeRequest(ctx: ChannelHandlerContext): Unit = {
    // flush
    ctx.flush()
    ctx.channel().flush()
    // close this channel
    ctx.channel().close()
  }

  private def closeChannels(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
    // Close the current channel
    ctx.channel.close
    // Then close the parent channel (the one attached to the bind)
    ctx.channel.parent.close
  }
}

class RestServerSource(ctx: SourceFunction.SourceContext[Map[String, Any]], override val port: Int)
  extends RestServer(port) {
  override protected def initChannel(ch: Channel): Unit = {
    ch.pipeline.addLast(new HttpServerCodec())
    ch.pipeline.addLast(new HttpObjectAggregator(1048576))
    ch.pipeline.addLast(new CommandsHandler(ctx))
  }

  def waitForExit(millis: Long): Boolean = {
    webThread.join(millis)
    webThread.isAlive
  }
}