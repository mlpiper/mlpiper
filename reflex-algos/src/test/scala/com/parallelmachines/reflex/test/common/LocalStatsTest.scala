package com.parallelmachines.reflex.test.common

import com.parallelmachines.reflex.common.InfoType
import org.junit.runner.RunWith
import org.mlpiper.stats._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class LocalStatsTest extends FlatSpec with Matchers {

  behavior of "LocalStats computing the correct result"

  def doubleData: Array[Double] = Array(2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0)

  def longData: Array[Long] = Array(2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)

  it should "Test WeightedAverageLocalStat correctly" in {

    val statInfo = StatInfo("average_test", StatPolicy.WEIGHTED_AVERAGE, StatPolicy.SUM)
    val startingValue = 1.0

    val stat = statInfo
      .toGlobalStat(startingValue, accumDataType = AccumData.getGraphType(startingValue),
        infoType = InfoType.InfoType.General)
    for (d <- doubleData) {
      stat.localUpdate(d)
    }

    stat.localValue should be(5.0 +- 1e-6)
  }

  it should "Test ReplaceLocalStat correctly" in {
    val statInfo = StatInfo("replace_test", StatPolicy.REPLACE, StatPolicy.SUM)
    val startingValue = 1L

    val stat = statInfo
      .toGlobalStat(startingValue, accumDataType = AccumData.getGraphType(startingValue),
        infoType = InfoType.InfoType.General)
    longData.foreach(x => {
      stat.localUpdate(x)
      stat.localValue should be(x)
    })
  }

  it should "Test SumLocalStat correctly" in {
    val statInfo = StatInfo("sum_test", StatPolicy.SUM, StatPolicy.SUM)
    val startingValue = 1L

    val stat = statInfo
      .toGlobalStat(startingValue, accumDataType = AccumData.getGraphType(startingValue),
        infoType = InfoType.InfoType.General)
    for (d <- longData) {
      stat.localUpdate(d)
    }

    stat.localValue should be(45L)
  }

  it should "Test MaxLocalStat correctly" in {
    val statInfo = StatInfo("max_test", StatPolicy.MAX, StatPolicy.MAX)
    val startingValue = 1L

    val stat = statInfo
      .toGlobalStat(startingValue, accumDataType = AccumData.getGraphType(startingValue),
        infoType = InfoType.InfoType.General)
    for (d <- longData) {
      stat.localUpdate(d)
    }

    stat.localValue should be(9L)
  }
}
