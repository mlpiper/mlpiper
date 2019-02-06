package com.parallelmachines.reflex.web.handler

import com.parallelmachines.reflex.web.helper.HandlerHelper
import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.{HttpMethod, HttpRequest, QueryStringDecoder}
import io.netty.util.CharsetUtil
import org.slf4j.LoggerFactory


class CommandsHandler extends SimpleChannelInboundHandler[HttpRequest] {
  private val logger = LoggerFactory.getLogger(getClass)

  override def channelRead0(ctx: ChannelHandlerContext, msg: HttpRequest): Unit = {
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
                  logger.debug("Handle 'exit' command ...")
                  HandlerHelper.writePlainTextResponse(
                    Unpooled.copiedBuffer("Done", CharsetUtil.UTF_8), ctx)
                  closeChannels(ctx)
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

  private def closeChannels(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
    // Close the current channel
    ctx.channel.close
    // Then close the parent channel (the one attached to the bind)
    ctx.channel.parent.close
  }
}
