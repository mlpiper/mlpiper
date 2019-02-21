package com.parallelmachines.reflex.pipeline.spark.stats

import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils
import org.apache.spark.scheduler.AccumulableInfo
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer


object AccumulatorStats {
  private val logger = LoggerFactory.getLogger(getClass)
  private val accumulatorStats = ListBuffer[String]()

  implicit val formats = DefaultFormats

  def setAccumulators(accumulators: Iterable[AccumulableInfo]): Unit = {
    for (accumInfo <- accumulators) {
      val metrics = collection.mutable.HashMap[String, String]()
      if (accumInfo.name.isDefined && accumInfo.value.isDefined) {
        val value = accumInfo.value.get.toString.trim
        if (value.matches("(\\{|\\[).*")) { // Filter out an absolute non-json values (in efficient way)
          metrics("name") = accumInfo.name.get
          metrics("value") = value
        }
      }

      if (metrics.nonEmpty) {
        var metricsJson: String = null

        accumulatorStats.synchronized {
          metricsJson = ParsingUtils.iterableToJSON(metrics)
          accumulatorStats += metricsJson
        }
        logger.debug(s"===========> $metricsJson")
      }
    }
  }

  def toListOfJsons(accumulators: Iterable[AccumulableInfo]): List[String] = {
    val accumulatorsLst = ListBuffer[String]()
    for (accumInfo <- accumulators) {
      val metrics = collection.mutable.HashMap[String, String]()
      if (accumInfo.name.isDefined && accumInfo.value.isDefined) {
        val value = accumInfo.value.get.toString.trim
        if (value.matches("(\\{|\\[).*")) { // Filter out an absolute non-json values (in efficient way)
          metrics("name") = accumInfo.name.get
          metrics("value") = value
        }
      }

      if (metrics.nonEmpty) {
        var metricsJson: String = ParsingUtils.iterableToJSON(metrics)
        accumulatorsLst += metricsJson
        logger.debug(s"===========> $metricsJson")
      }
    }
    accumulatorsLst.toList
  }


  def get(): String = {
    var json = ""
    accumulatorStats.synchronized {
      json = write(accumulatorStats)
      accumulatorStats.clear()
    }
    json
  }
}