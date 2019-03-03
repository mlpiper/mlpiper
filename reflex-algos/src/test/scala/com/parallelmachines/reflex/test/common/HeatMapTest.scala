package com.parallelmachines.reflex.test.common

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.mlpiper.stat.heatmap.continuous.HeatMap
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
