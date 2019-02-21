package com.parallelmachines.reflex.test.common

import breeze.linalg.DenseVector
import com.parallelmachines.reflex.common.{ContinuousHistogramForFlink, HealthType}
import org.apache.flink.contrib.streaming.scala.utils.DataStreamUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.scala.examples.clustering.stat.continuous._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class ContinuousHistogramTest extends FlatSpec with Matchers {

  /**
    * Testing compare functionality of joined featured histograms
    */
  it should "Correctly Generate Overlap Score For Joined Featured Hist" in {
    val featureID1 = "0"
    val featureID2 = "1"

    val hist1ForFeature1 = new Histogram(histVector = new DenseVector[Double](Array(10.0, 6.0, 4.0)), binEdgesVector = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0)))
    val hist1ForFeature2 = new Histogram(histVector = new DenseVector[Double](Array(15.0, 93.0)), binEdgesVector = new DenseVector[Double](Array(4.0, 7.0, 10.0)))

    val hist2ForFeature1 = new Histogram(histVector = new DenseVector[Double](Array(15.0, 6.0, 24.0)), binEdgesVector = new DenseVector[Double](Array(2.0, 5.0, 8.0, 11.0)))
    val hist2ForFeature2 = new Histogram(histVector = new DenseVector[Double](Array(62.0, 31.0)), binEdgesVector = new DenseVector[Double](Array(6.0, 7.0, 8.0)))

    // first map containing featureID and hist
    val map1OfFeatureIDAndHist = mutable.Map[String, Histogram]()
    map1OfFeatureIDAndHist += (featureID1 -> hist1ForFeature1)
    map1OfFeatureIDAndHist += (featureID2 -> hist1ForFeature2)

    // second map containing featureID and hist
    val map2OfFeatureIDAndHist = mutable.Map[String, Histogram]()
    map2OfFeatureIDAndHist += (featureID1 -> hist2ForFeature1)
    map2OfFeatureIDAndHist += (featureID2 -> hist2ForFeature2)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val joinedHist = env.fromElements((map1OfFeatureIDAndHist, map2OfFeatureIDAndHist))

    val continuousHistogramForFlink = new ContinuousHistogramForFlink(HealthType.ContinuousHistogramHealth.toString)
    continuousHistogramForFlink.joinedHistStream = joinedHist
    continuousHistogramForFlink.enableAccumOutputOfHistogramsWithScore = false

    val overlapScoreStream = continuousHistogramForFlink.compareHealth()

    val overlapScore = DataStreamUtils(overlapScoreStream).collect().toSeq.head

    val scoreArray = overlapScore.score

    val expectedScoreForFeature1 = 0.44008
    val expectedScoreForFeature2 = 0.29939

    scoreArray(featureID1) should be(expectedScoreForFeature1 +- 1e-2)
    scoreArray(featureID2) should be(expectedScoreForFeature2 +- 1e-2)
  }
}
