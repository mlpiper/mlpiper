package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import breeze.linalg.DenseVector
import org.apache.flink.contrib.streaming.scala.utils.DataStreamUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.scala.examples.common.stats.AccumulatorInfoJsonHeaders
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.junit.runner.RunWith
import org.mlpiper.stat.histogram.continuous.{Histogram, PredictionHistogram}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.{immutable, mutable}

@RunWith(classOf[JUnitRunner])
class PredictionHistogramTest extends FlatSpec with Matchers {
  /**
    * Testing Correct Generation Of IDedFeaturedHistogram
    */
  it should "Generate Correct IDedFeaturedHistogram" in {
    val labelID = "0"

    val hist1 = new Histogram(histVector = new DenseVector[Double](Array(10.0, 6.0, 4.0)), binEdgesVector = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0)))
    val hist2 = new Histogram(histVector = new DenseVector[Double](Array(15.0, 93.0)), binEdgesVector = new DenseVector[Double](Array(4.0, 7.0, 10.0)))
    val hist3 = new Histogram(histVector = new DenseVector[Double](Array(15.0, 6.0, 24.0)), binEdgesVector = new DenseVector[Double](Array(2.0, 5.0, 8.0, 11.0)))
    val hist4 = new Histogram(histVector = new DenseVector[Double](Array(62.0, 31.0)), binEdgesVector = new DenseVector[Double](Array(6.0, 7.0, 8.0)))

    val map1OfFeatureIDAndHist = mutable.Map[String, Histogram](labelID -> hist1)
    val map2OfFeatureIDAndHist = mutable.Map[String, Histogram](labelID -> hist2)
    val map3OfFeatureIDAndHist = mutable.Map[String, Histogram](labelID -> hist3)
    val map4OfFeatureIDAndHist = mutable.Map[String, Histogram](labelID -> hist4)

    val mapsSeq: Seq[mutable.Map[String, Histogram]] = Seq[mutable.Map[String, Histogram]](
      map1OfFeatureIDAndHist,
      map2OfFeatureIDAndHist,
      map3OfFeatureIDAndHist,
      map4OfFeatureIDAndHist
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val streamOfFeaturedHist = env.fromCollection(mapsSeq)

    val generatedIDedFeatHist = PredictionHistogram.generateIDedFeaturedHist(streamOfFeaturedHist)

    val seqOfOutput = DataStreamUtils(generatedIDedFeatHist).collect().toSeq

    val seqOfExpectedOutput: Seq[PredictionHistogram] = Seq[PredictionHistogram](
      PredictionHistogram(ID = 1L, map1OfFeatureIDAndHist),
      PredictionHistogram(ID = 2L, map2OfFeatureIDAndHist),
      PredictionHistogram(ID = 3L, map3OfFeatureIDAndHist),
      PredictionHistogram(ID = 4L, map4OfFeatureIDAndHist)
    )

    seqOfOutput.foreach(x =>
      seqOfExpectedOutput.contains(x) should be(true)
    )
  }

  /**
    * Testing [[PredictionHistogram]] to JSON format functionality
    */
  it should "Test JSON Conversion" in {
    val labelID = "0"

    val hist1 = new Histogram(histVector = new DenseVector[Double](Array(10.0, 6.0, 4.0)), binEdgesVector = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0)))
    val map1OfFeatureIDAndHist = mutable.Map[String, Histogram](labelID -> hist1)

    val idedFeatureHist = PredictionHistogram(ID = 1L, map1OfFeatureIDAndHist)

    val idedFeatureHistogramJSON = PredictionHistogram.toIDedFeatureHistogramJSON(idedFeaturedHistogram = idedFeatureHist)

    val expectedConversion =
      "{\"value\":\"{\\\"type\\\":\\\"Health\\\",\\\"data\\\":\\\"{\\\\\\\"0\\\\\\\":[{\\\\\\\"1.0 to 3.0\\\\\\\":10.0},{\\\\\\\"3.0 to 5.0\\\\\\\":6.0},{\\\\\\\"5.0 to 7.0\\\\\\\":4.0}]}\\\",\\\"graphType\\\":\\\"BARGRAPH\\\",\\\"mode\\\":\\\"INSTANT\\\",\\\"name\\\":\\\"PredictionHistogram\\\",\\\"timestamp\\\":\\\"1519421816261\\\",\\\"id\\\":\\\"1\\\",\\\"modelId\\\":null}\",\"name\":\"PredictionHistogram\"}"
    val expectedMap: immutable.Map[String, String] =
      Json(DefaultFormats)
        .parse(expectedConversion)
        .values
        .asInstanceOf[immutable.Map[String, String]]
    val expectedHist: immutable.Map[String, String] =
      Json(DefaultFormats)
        .parse(expectedMap(PredictionHistogram.Value))
        .values
        .asInstanceOf[immutable.Map[String, String]]

    val actualMap: immutable.Map[String, String] =
      Json(DefaultFormats)
        .parse(idedFeatureHistogramJSON)
        .values
        .asInstanceOf[immutable.Map[String, String]]
    val actualHist: immutable.Map[String, String] =
      Json(DefaultFormats)
        .parse(actualMap(PredictionHistogram.Value))
        .values
        .asInstanceOf[immutable.Map[String, String]]

    actualMap.size == expectedMap.size should be(true)
    actualHist.size == expectedHist.size should be(true)
    actualHist(PredictionHistogram.ID).equals(expectedHist(PredictionHistogram.ID)) should be(true)
    actualHist(AccumulatorInfoJsonHeaders.DataKey).equals(expectedHist(AccumulatorInfoJsonHeaders.DataKey)) should be(true)
  }


  /**
    * Testing JSON to [[PredictionHistogram]] format functionality
    */
  it should "Test IDedFeaturedHistogram Conversion" in {
    val idedFeatureHistogramJSON =
      "{\"value\":\"{\\\"type\\\":\\\"Health\\\",\\\"data\\\":\\\"{\\\\\\\"0\\\\\\\":[{\\\\\\\"1.0 to 3.0\\\\\\\":10.0},{\\\\\\\"3.0 to 5.0\\\\\\\":6.0},{\\\\\\\"5.0 to 7.0\\\\\\\":4.0}]}\\\",\\\"graphType\\\":\\\"BARGRAPH\\\",\\\"mode\\\":\\\"INSTANT\\\",\\\"name\\\":\\\"PredictionHistogram\\\",\\\"timestamp\\\":\\\"1519752326150\\\",\\\"id\\\":\\\"1\\\"}\",\"name\":\"PredictionHistogram\"}"

    val labelID = "0"

    val hist1 = new Histogram(histVector = new DenseVector[Double](Array(10.0, 6.0, 4.0)), binEdgesVector = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0)))
    val map1OfFeatureIDAndHist = mutable.Map[String, Histogram](labelID -> hist1)

    val expectedIDedFeatureHist = PredictionHistogram(ID = 1L, map1OfFeatureIDAndHist)

    val generatedIDedFeatureHist = PredictionHistogram.fromIDedFeatureHistogramJSON(idedFeatureHistogramJSON)

    generatedIDedFeatureHist.equals(expectedIDedFeatureHist) should be(true)
  }
}
