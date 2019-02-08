
package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import com.parallelmachines.reflex.common.InfoType
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.scala.examples.common.stats._
import org.apache.flink.streaming.scala.examples.flink.utils.functions.source.RoundRobinIterableSourceFunction
import org.apache.flink.streaming.test.exampleScalaPrograms.clustering.GlobalStatsTest.GlobalStatMapNumeric
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

object GlobalStatsTest {

  class GlobalStatMapNumeric[T](statInfo: StatInfo) extends RichMapFunction[T, T] {

    var globalStat: GlobalAccumulator[T] = _

    override def map(value: T): T = {
      if (globalStat != null) {
        globalStat.localUpdate(value)
      } else {
        globalStat = statInfo.toGlobalStat(value, accumDataType = AccumData.getGraphType(value),
          infoType = InfoType.InfoType.General)
      }
      globalStat.updateFlinkAccumulator(this.getRuntimeContext)
      value
    }
  }

}

@RunWith(classOf[JUnitRunner])
class GlobalStatsTest extends FlatSpec with Matchers {

  behavior of "GlobalStats computing the correct result in FlinkAccumulators"

  def doubleData1: ListBuffer[Double] = ListBuffer(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0)

  def doubleData2: ListBuffer[Double] = ListBuffer(7.0, 7.0, 7.0, 7.0, 8.0, 8.0, 8.0, 8.0)

  def longData: ListBuffer[Long] = ListBuffer(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)

  it should "Test WeightedAverageGlobalStat correctly" in {
    val statInfo = StatInfo("weighted_average_test", StatPolicy.WEIGHTED_AVERAGE, StatPolicy.WEIGHTED_AVERAGE)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val stream = env.addSource(new RoundRobinIterableSourceFunction[Double](doubleData1, 0, 0))
    stream.map(new GlobalStatMapNumeric[Double](statInfo))
    val job = env.execute()

    val result = job.getAccumulatorResult[AccumulatorInfo[Double]]("weighted_average_test").value
    result should be(5.0)
  }

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

  it should "Test AverageGlobalStat correctly" in {
    val statInfo = StatInfo("average_test", StatPolicy.REPLACE, StatPolicy.AVERAGE)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val stream = env.addSource(new RoundRobinIterableSourceFunction[Double](doubleData2, 0, 0))
    stream.map(new GlobalStatMapNumeric[Double](statInfo))
    val job = env.execute()

    val result = job.getAccumulatorResult[AccumulatorInfo[Double]]("average_test").value
    result should be(8.0)
  }

  it should "Test SumGlobalStat correctly" in {
    val statInfo = StatInfo("sum_test", StatPolicy.SUM, StatPolicy.SUM)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val stream = env.addSource(new RoundRobinIterableSourceFunction[Long](longData, 0, 0))
    stream.map(new GlobalStatMapNumeric[Long](statInfo))
    val job = env.execute()

    val result = job.getAccumulatorResult[AccumulatorInfo[Long]]("sum_test").value
    result should be(45L)
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

  it should "Test MaxGlobalStat correctly" in {
    val statInfo = StatInfo("max_test", StatPolicy.MAX, StatPolicy.MAX)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val stream = env.addSource(new RoundRobinIterableSourceFunction[Long](longData, 0, 0))
    stream.map(new GlobalStatMapNumeric[Long](statInfo))
    val job = env.execute()

    val result = job.getAccumulatorResult[AccumulatorInfo[Long]]("max_test").value
    result should be(9L)
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
