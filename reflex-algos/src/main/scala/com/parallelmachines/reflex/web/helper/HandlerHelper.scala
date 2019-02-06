package com.parallelmachines.reflex.web.helper

import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext}
import io.netty.handler.codec.http._


object HandlerHelper {

  def writeResponse(content: ByteBuf, ctx: ChannelHandlerContext) : Unit = {
    val response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content)
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes())
    ctx.write(response)
  }

  def writePlainTextResponse(content: ByteBuf, ctx: ChannelHandlerContext) : Unit = {
    val response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content)
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_OCTET_STREAM)
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes())
    ctx.write(response)
  }

  def writeResponseNotFound(ctx: ChannelHandlerContext) : Unit = {
    ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND))
      .addListener(ChannelFutureListener.CLOSE)
  }
}
