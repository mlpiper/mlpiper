package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import breeze.linalg.DenseVector
import org.apache.flink.contrib.streaming.scala.utils.DataStreamUtils
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.scala.examples.clustering.math.{DenseVectorToNamedVectorFlatMap, ReflexNamedVector}
import org.apache.flink.streaming.scala.examples.clustering.stat.heatmap.{HeatMap, HeatMapMethod, HeatMapValues}
import org.apache.flink.streaming.scala.examples.common.stats.{AccumulatorInfo, StatTable}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class HeatMapTest extends FlatSpec with Matchers {

  def compareTwoMaps(eachHeatMapVal: Map[String, Double],
                     eachExpectedHeatValue: Map[String, Double]): Boolean = {
    var foundInVals = true

    eachExpectedHeatValue.keys.foreach(
      eachKey => {
        val valFound = ComparatorUtils.compareDouble(a = eachHeatMapVal(eachKey), b = eachExpectedHeatValue(eachKey), epsilon = 0.01)
        foundInVals = foundInVals && valFound
      })

    foundInVals
  }

  def compareTwoMapsSeq(a: Seq[Map[String, Double]], b: Seq[Map[String, Double]]): Unit = {
    a.foreach(eachHeatMapVal => {
      var found = false
      b.foreach(eachExpectedHeatValue => {
        val foundInVals = compareTwoMaps(eachHeatMapVal, eachExpectedHeatValue)

        found = found || foundInVals
      })

      found should be(true)
    })
  }

  /**
    * Testing HeatMap Generation
    */
  it should "Generate Correct HeatMap From DataStream" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val denseVectors: Seq[DenseVector[Double]] = HeatMapTestData.testDataStreamForHeatMap

    val streamOfVectors: DataStream[DenseVector[Double]] = env.fromCollection(denseVectors)

    // calculating heatmap for given stream of vectors by using "local-by-norm-mean" methodology
    val heatMapValues = HeatMap
      .createHeatMap(
        stream = streamOfVectors.flatMap(new DenseVectorToNamedVectorFlatMap()),
        localHeatMapMethod = HeatMapMethod.LocalByNormMean,
        localWindowSize = 4,
        globalHeatMapMethod = None,
        globalWindowSize = None,
        doubleWindowing = false)

    val heatMapValuesDataStreamSeq: Seq[Map[String, Double]] = DataStreamUtils(heatMapValues).collect().toSeq.map(_.heatMapValue)

    val expectedHeatMapValuesDataStreamSeq: Seq[Map[String, Double]] = HeatMapTestData.expectedMeanHeatMap

    compareTwoMapsSeq(a = heatMapValuesDataStreamSeq, b = expectedHeatMapValuesDataStreamSeq)
  }

  /**
    * Testing HeatMap Generation in Accumulator
    */
  it should "Generate Correct HeatMap From Accumulator" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val denseVectors: Seq[DenseVector[Double]] = HeatMapTestData.testDataStreamForHeatMap

    val streamOfVectors: DataStream[DenseVector[Double]] = env.fromCollection(denseVectors)

    // calculating heatmap for given stream of vectors by using "local-by-norm-mean" methodology
    HeatMap
      .createHeatMap(
        stream = streamOfVectors.flatMap(new DenseVectorToNamedVectorFlatMap()),
        localHeatMapMethod = HeatMapMethod.LocalByNormMean,
        localWindowSize = 4,
        globalHeatMapMethod = None,
        globalWindowSize = None,
        doubleWindowing = false)

    val job = env.execute()

    val heatMapValue = job
      .getAccumulatorResult[AccumulatorInfo[HeatMapValues]](StatTable.DATA_HEATMAP.toString)
      .value
      .heatMapValue

    val expectedHeatMapValue = HeatMapTestData.expectedMeanHeatMap.last

    compareTwoMaps(heatMapValue, expectedHeatMapValue) should be(true)
  }

  /**
    * Testing HeatMap Generation For Window Of Two
    */
  it should "Generate Correct HeatMap From DataStream With Window Of 2" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val denseVectors: Seq[DenseVector[Double]] = HeatMapTestData.testDataStreamForHeatMap

    val streamOfVectors: DataStream[DenseVector[Double]] = env.fromCollection(denseVectors)

    // calculating heatmap for given stream of vectors by using "local-by-norm-mean" methodology
    val heatMapValues = HeatMap
      .createHeatMap(
        stream = streamOfVectors.flatMap(new DenseVectorToNamedVectorFlatMap()),
        localHeatMapMethod = HeatMapMethod.LocalByNormMean,
        localWindowSize = 2,
        globalHeatMapMethod = None,
        globalWindowSize = None,
        doubleWindowing = false)

    val heatMapValuesDataStreamSeq: Seq[Map[String, Double]] = DataStreamUtils(heatMapValues).collect().toSeq.map(_.heatMapValue)
    val expectedHeatMapValuesDataStreamSeq: Seq[Map[String, Double]] = HeatMapTestData.expectedMeanHeatMapForWindowOfTwo

    compareTwoMapsSeq(heatMapValuesDataStreamSeq, expectedHeatMapValuesDataStreamSeq)
  }

  /**
    * Testing HeatMap Generation For Constants
    */
  it should "Generate Correct HeatMap From DataStream Of Constant" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val denseVectors: Seq[DenseVector[Double]] = HeatMapTestData.testDataStreamOfConstantsForHeatMap

    val streamOfVectors: DataStream[DenseVector[Double]] = env.fromCollection(denseVectors)

    // calculating heatmap for given stream of vectors by using "local-by-norm-mean" methodology
    val heatMapValues = HeatMap
      .createHeatMap(
        stream = streamOfVectors.flatMap(new DenseVectorToNamedVectorFlatMap()),
        localHeatMapMethod = HeatMapMethod.LocalByNormMean,
        localWindowSize = 4,
        globalHeatMapMethod = None,
        globalWindowSize = None,
        doubleWindowing = false)

    val heatMapValuesDataStreamSeq: Seq[Map[String, Double]] = DataStreamUtils(heatMapValues).collect().toSeq.map(_.heatMapValue)
    val expectedHeatMapValuesDataStreamSeq: Seq[Map[String, Double]] = HeatMapTestData.expectedMeanHeatMapOfConstants

    compareTwoMapsSeq(heatMapValuesDataStreamSeq, expectedHeatMapValuesDataStreamSeq)
  }

  /**
    * Testing HeatMap Generation
    */
  it should "Generate Correct HeatMap From DataStream For Higher Parallelism" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val denseVectors: Seq[DenseVector[Double]] = HeatMapTestData.testDataStreamForHeatMapForHigherParallism

    val streamOfVectors: DataStream[DenseVector[Double]] = env.fromCollection(denseVectors)

    // calculating heatmap for given stream of vectors by using "local-by-norm-mean" methodology
    val heatMapValues = HeatMap
      .createHeatMap(
        stream = streamOfVectors.flatMap(new DenseVectorToNamedVectorFlatMap()),
        localHeatMapMethod = HeatMapMethod.LocalByNormMean,
        localWindowSize = 4,
        globalHeatMapMethod = None,
        globalWindowSize = None,
        doubleWindowing = false)

    val heatMapValuesDataStreamSeq: Seq[Map[String, Double]] = DataStreamUtils(heatMapValues).collect().toSeq.map(_.heatMapValue)
    val expectedHeatMapValuesDataStreamSeq: Seq[Map[String, Double]] = HeatMapTestData.expectedMeanHeatMap

    compareTwoMapsSeq(heatMapValuesDataStreamSeq, expectedHeatMapValuesDataStreamSeq)
  }

  /**
    * Testing HeatMap Generation in Accumulator
    */
  it should "Generate Correct HeatMap From Accumulator For Higher Parallelism" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val denseVectors: Seq[DenseVector[Double]] = HeatMapTestData.testDataStreamForHeatMapForHigherParallism

    val streamOfVectors: DataStream[DenseVector[Double]] = env.fromCollection(denseVectors)

    // calculating heatmap for given stream of vectors by using "local-by-norm-mean" methodology
    HeatMap
      .createHeatMap(
        stream = streamOfVectors.flatMap(new DenseVectorToNamedVectorFlatMap()),
        localHeatMapMethod = HeatMapMethod.LocalByNormMean,
        localWindowSize = 4,
        globalHeatMapMethod = None,
        globalWindowSize = None,
        doubleWindowing = false)

    val job = env.execute()

    val heatMapValue = job
      .getAccumulatorResult[AccumulatorInfo[HeatMapValues]](StatTable.DATA_HEATMAP.toString)
      .value
      .heatMapValue

    // expected value should be average of HeatMaps
    val expectedHeatMapValue = HeatMapTestData.expectedMeanHeatMap.map(HeatMapValues(_)).reduce(_ + _)

    compareTwoMaps(heatMapValue, expectedHeatMapValue.heatMapValue)
  }

  /**
    * Testing HeatMap Generation For Double Windowing
    * First Window - Mean
    * Second Window - MinMax Scale
    */
  it should "Generate Correct HeatMap From DataStream For Double Windowing" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val denseVectors: Seq[DenseVector[Double]] = HeatMapTestData.testDataStreamForHeatMap

    val streamOfVectors: DataStream[DenseVector[Double]] = env.fromCollection(denseVectors)

    // calculating heatmap for given stream of vectors by using "local-by-norm-mean" methodology
    val heatMapValues = HeatMap
      .createHeatMap(
        stream = streamOfVectors.flatMap(new DenseVectorToNamedVectorFlatMap()),
        localHeatMapMethod = HeatMapMethod.LocalByMean,
        localWindowSize = 2L,
        globalHeatMapMethod = Some(HeatMapMethod.GlobalByMinMaxScale),
        globalWindowSize = Some(3L),
        doubleWindowing = true)

    val heatMapValuesDataStreamSeq: Seq[Map[String, Double]] = DataStreamUtils(heatMapValues).collect().toSeq.map(_.heatMapValue)
    val expectedHeatMapValuesDataStreamSeq: Seq[Map[String, Double]] = HeatMapTestData.expectedMeanHeatMapForDoubleWindow_Mean_MinMax

    compareTwoMapsSeq(heatMapValuesDataStreamSeq, expectedHeatMapValuesDataStreamSeq)
  }

  /**
    * Testing HeatMap Generation For Double Windowing
    * First Window - Mean
    * Second Window - Standard Scale
    */
  it should "Generate Correct HeatMap From DataStream For Double Windowing Mean + Standard" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val denseMatrixes: Seq[DenseVector[Double]] = HeatMapTestData.testDataStreamForHeatMap

    val streamOfVectors: DataStream[DenseVector[Double]] = env.fromCollection(denseMatrixes)

    // calculating heatmap for given stream of vectors by using "local-by-norm-mean" methodology
    val heatMapValues = HeatMap
      .createHeatMap(
        stream = streamOfVectors.flatMap(new DenseVectorToNamedVectorFlatMap()),
        localHeatMapMethod = HeatMapMethod.LocalByMean,
        localWindowSize = 2L,
        globalHeatMapMethod = Some(HeatMapMethod.GlobalByStandardScale),
        globalWindowSize = Some(3L),
        doubleWindowing = true)

    val heatMapValuesDataStreamSeq: Seq[Map[String, Double]] = DataStreamUtils(heatMapValues).collect().toSeq.map(_.heatMapValue)
    val expectedHeatMapValuesDataStreamSeq: Seq[Map[String, Double]] = HeatMapTestData.expectedMeanHeatMapForDoubleWindow_Mean_Standard

    compareTwoMapsSeq(heatMapValuesDataStreamSeq, expectedHeatMapValuesDataStreamSeq)
  }

  /**
    * Testing HeatMap Generation For Double Windowing
    * First Window - Mean
    * Second Window - Standard Scale
    */
  it should "Generate Correct HeatMap From DataStream Of NV For Double Windowing Mean + Standard" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val denseMatrixes: Seq[ReflexNamedVector] = HeatMapTestData.testDataStreamOfNamedVectorForHeatMap

    val streamOfVectors: DataStream[ReflexNamedVector] = env.fromCollection(denseMatrixes)

    // calculating heatmap for given stream of vectors by using "local-by-norm-mean" methodology
    val heatMapValues = HeatMap
      .createHeatMap(
        stream = streamOfVectors,
        localHeatMapMethod = HeatMapMethod.LocalByMean,
        localWindowSize = 2L,
        globalHeatMapMethod = Some(HeatMapMethod.GlobalByStandardScale),
        globalWindowSize = Some(3L),
        doubleWindowing = true)

    val heatMapValuesDataStreamSeq: Seq[Map[String, Double]] = DataStreamUtils(heatMapValues).collect().toSeq.map(_.heatMapValue)
    val expectedHeatMapValuesDataStreamSeq: Seq[Map[String, Double]] = HeatMapTestData.expectedMeanHeatMapFor_NamedVector_DoubleWindow_Mean_Standard

    compareTwoMapsSeq(heatMapValuesDataStreamSeq, expectedHeatMapValuesDataStreamSeq)
  }

  /**
    * Testing HeatMap Generation For Spark Batch Of Named Vectors
    * Normalized Mean HeatMaps
    */
  it should "Generate Correct HeatMap From RDD Of NV For Spark" in {
    val sparkSession = SparkSession.builder.
      master("local[*]").appName("Histograms API Test").getOrCreate()

    val sc = sparkSession.sparkContext

    val rddOfNV = sc.parallelize(HeatMapTestData.testDataStreamOfNamedVectorForHeatMap, 4)

    // calculating heatmap for given rdd of vectors by using "local-by-norm-mean" methodology
    val heatMapValues: Map[String, Double] = HeatMap
      .createHeatMap(
        rddOfNamedVec = rddOfNV,
        env = sc
      ).get.heatMapValue

    val expectedHeatMapValuesSeq: Map[String, Double] = Map("A" -> 0.4375, "B" -> 0.4719, "C" -> 0.55)

    expectedHeatMapValuesSeq.foreach(eachTuple => {
      eachTuple._2 should be(heatMapValues(eachTuple._1) +- 1e-2)
    })

    sc.stop()
  }
}
