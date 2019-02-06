package com.parallelmachines.reflex.web.handler

import com.parallelmachines.reflex.pipeline.spark.stats.{AccumulatorStats, SystemStats}
import com.parallelmachines.reflex.web.helper.HandlerHelper
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http._
import io.netty.util.CharsetUtil
import org.slf4j.LoggerFactory


@Sharable
class StatisticsHandler() extends SimpleChannelInboundHandler[HttpRequest] {
  private val logger = LoggerFactory.getLogger(getClass)

  override def channelRead0(ctx: ChannelHandlerContext, msg: HttpRequest): Unit = {
    logger.debug(s"Method: ${msg.method()}, Uri: ${msg.uri()}")
    msg.method match {
      case HttpMethod.GET =>
        logger.debug(s"RestApi.AccumStats: ${RestApi.AccumStats}, msg.uri(): ${msg.uri()}")
        msg.uri() match {
          case RestApi.StatsPath =>
            logger.debug("Handel 'get' of all stats ...")
            val jsonSysStats = SystemStats.get()
            val jsonAccumStats = AccumulatorStats.get()
            val allStats = s"""{"system": ${jsonSysStats} , "accumulators": ${jsonAccumStats}}"""
            writeJsonResponse(allStats, ctx)
          case RestApi.SystemStats =>
            logger.debug("Handle 'get' system statistics ...")
            val json = SystemStats.get()
            writeJsonResponse(json, ctx)
          case RestApi.AccumStats =>
            logger.debug("Handle 'get' accumulators statistics ...")
            val json = AccumulatorStats.get()
            writeJsonResponse(json, ctx)
          case _ => HandlerHelper.writeResponseNotFound(ctx)
        }
      case _ => ctx.fireChannelRead(msg)
    }
  }

  def writeJsonResponse(json: String, ctx: ChannelHandlerContext): Unit = {
    val content = Unpooled.copiedBuffer(json, CharsetUtil.UTF_8)
    HandlerHelper.writeResponse(content, ctx)
  }

  @throws[Exception]
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }
}
