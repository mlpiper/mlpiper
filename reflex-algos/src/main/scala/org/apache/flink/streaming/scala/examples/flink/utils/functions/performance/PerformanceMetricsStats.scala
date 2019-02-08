package org.apache.flink.streaming.scala.examples.flink.utils.functions.performance

import com.parallelmachines.reflex.common.InfoType._
import org.apache.flink.streaming.scala.examples.common.stats.{AccumData, GlobalAccumulator, StatInfo}
import org.slf4j.LoggerFactory

import scala.collection.mutable

trait PerformanceMetricsStats extends Serializable {

  private val logger = LoggerFactory.getLogger(getClass)

  protected val globalStats = new mutable.HashMap[String, GlobalAccumulator[Any]]

  /** To be called before any computation is performed. */
  protected def open(): Unit = {}

  /** To be called on every input, before any computation of the input is performed. */
  protected def onInput(): Unit = {}

  /** Trigger that defines when to call [[storeMetrics]]. */
  protected def printTrigger(): Boolean = false

  /** Called when [[printTrigger]] returns true. */
  protected def storeMetrics(): Option[PerformanceMetricsHash] = {
    None
  }

  /**
    * @param block Block of code to time.
    * @tparam T Block output.
    * @return (Block output, Block duration in nanoseconds)
    */
  final protected def time[T](block: => T): OutputWithTime[T] = {
    val t0 = System.nanoTime()
    val res = block
    val elapsedTime = System.nanoTime() - t0
    new OutputWithTime[T](res, elapsedTime)
  }

  /** Should be called one every input. Checks to see if [[printTrigger]] returns true, if so,
    * call [[storeMetrics]]. */
  final protected def printMetricsIfTriggered(): Option[PerformanceMetricsHash] = {
    if (printTrigger()) {
      val metrics = storeMetrics()
      if (metrics.isDefined) {
        this.updateLocalStats(metrics.get)
      }
      metrics
    } else {
      None
    }
  }

  private def updateLocalStats(stats: PerformanceMetricsHash): Unit = {
    stats.metrics.foreach(stat => updateLocalStats(stat))
  }

  protected def updateLocalStats(stats: (StatInfo, Any)*): Unit = {
    val precision = 4
    stats.foreach(stat => {
      try {
        val statInfo = stat._1

        val value = stat._2 match {
          case doubleVal: Double => BigDecimal.decimal(doubleVal).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
          case floatVal: Float => BigDecimal.decimal(floatVal).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toFloat
          case _ => stat._2
        }

        if (globalStats.contains(statInfo.name)) {
          globalStats(statInfo.name).localUpdate(value)
        } else {
          globalStats.put(statInfo.name, statInfo.toGlobalStat(value, accumDataType = AccumData.getGraphType(value),
            infoType = InfoType.General))
        }
      }

      catch {
        case e: Exception =>
          logger.error(s"It is not possible to update stats for ${stat._1.name} with value of ${stat._2}. Error: ${e.getMessage}")
      }
    })
  }
}
