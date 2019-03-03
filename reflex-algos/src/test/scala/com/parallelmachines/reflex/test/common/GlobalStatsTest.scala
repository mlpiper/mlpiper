
package com.parallelmachines.reflex.test.common

import com.parallelmachines.reflex.common.InfoType
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.mlpiper.stats._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class GlobalStatsTest extends FlatSpec with Matchers {

  behavior of "GlobalStats computing the correct result in FlinkAccumulators"

  def doubleData1: ListBuffer[Double] = ListBuffer(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0)

  def doubleData2: ListBuffer[Double] = ListBuffer(7.0, 7.0, 7.0, 7.0, 8.0, 8.0, 8.0, 8.0)

  def longData: ListBuffer[Long] = ListBuffer(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)

  it should "Test WeightedAverageGlobalStat correctly in Spark" in {
    val master = "local[4]"
    val appName = "spark-weighted_average_test"

    val sparkSession = SparkSession.builder().master(master).appName(appName).getOrCreate()

    val sc = sparkSession.sparkContext

    val startingValue: Double = 0.0
    val accumulator = StatInfo("weighted_average_test", StatPolicy.WEIGHTED_AVERAGE, StatPolicy.WEIGHTED_AVERAGE)
      .toGlobalStat(startingValue, accumDataType = AccumData.getGraphType(startingValue),
        infoType = InfoType.InfoType.General)

    // Every partition's accumulator will have local value 0 with count 1, so we set local count to 0
    accumulator.setLocalCount(0L)

    // Initializes Spark accumulator with SparkContext
    accumulator.updateSparkAccumulator(sc)

    sc.parallelize(doubleData1)
      .map(x => {
        accumulator.localUpdate(x);
        accumulator.updateSparkAccumulator();
        x
      })
      .count()

    accumulator.getSparkAccumulator.value.value should be(5.0)

    sc.stop()
  }

  it should "Test SumGlobalStat correctly for Spark" in {
    val master = "local[4]"
    val appName = "spark-sum_test"

    val sparkSession = SparkSession.builder().master(master).appName(appName).getOrCreate()

    val sc = sparkSession.sparkContext

    val startingValue = 0L
    val accumulator = StatInfo("sum_test", StatPolicy.SUM, StatPolicy.SUM)
      .toGlobalStat(startingValue, accumDataType = AccumData.getGraphType(startingValue),
        infoType = InfoType.InfoType.General)

    // Initializes Spark accumulator with SparkContext
    accumulator.updateSparkAccumulator(sc)

    sc.parallelize(longData)
      .map(x => {
        accumulator.localUpdate(x);
        accumulator.updateSparkAccumulator();
        x
      })
      .count()

    accumulator.getSparkAccumulator.value.value should be(45L)

    sc.stop()
  }

  it should "Test MaxGlobalStat correctly for Spark" in {
    val master = "local[4]"
    val appName = "spark-max_test"

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    val sc = new SparkContext(conf)

    val startingValue = 0L
    val accumulator = StatInfo("max_test", StatPolicy.MAX, StatPolicy.MAX)
      .toGlobalStat(startingValue, accumDataType = AccumData.getGraphType(startingValue),
        infoType = InfoType.InfoType.General)

    // Initializes Spark accumulator with SparkContext
    accumulator.updateSparkAccumulator(sc)

    sc.parallelize(longData)
      .map(x => {
        accumulator.localUpdate(x);
        accumulator.updateSparkAccumulator();
        x
      })
      .count()

    accumulator.getSparkAccumulator.value.value should be(9L)

    sc.stop()
  }
}
