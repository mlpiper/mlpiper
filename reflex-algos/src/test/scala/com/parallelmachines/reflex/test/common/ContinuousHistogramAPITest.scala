package com.parallelmachines.reflex.test.common

import breeze.linalg.{DenseVector, sum}
import com.parallelmachines.reflex.common.enums.OpType
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.mlpiper.datastructures.{ColumnEntry, ColumnVectorEntry, NamedMatrix, NamedVector}
import org.mlpiper.stat.healthlib.{ContinuousHistogramForSpark, MinMaxRangeForHistogramType}
import org.mlpiper.stat.histogram.HistogramComparatorTypes
import org.mlpiper.stat.histogram.continuous._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class ContinuousHistogramAPITest extends FlatSpec with Matchers {
  /**
    * Testing Dense Vector to Histogram
    */
  it should "Generate Correct Histogram From Dense Vector Where Using FeaturedHistogramFromDenseVector" in {
    val denseVector1 = new DenseVector[Double](Array(2.0, 2.0, 2.0))

    val histogramDouble = FeaturedHistogramFromDenseVector.getHistogram(denseVector1).get

    // range should be 2 +- 0.01
    val expectedBinEdge = DenseVector[Double](Int.MinValue.toDouble +: Array(1.99, 2.01) :+ Int.MaxValue.toDouble)
    val expectedHist = DenseVector[Double](0.0, 3.0, 0.0)

    ComparatorUtils.compareBreezeOutputVector(a = histogramDouble.hist, b = expectedHist) should be(true)
    ComparatorUtils.compareBreezeOutputVector(a = histogramDouble.binEdges, b = expectedBinEdge) should be(true)
  }

  /**
    * Testing Dense Vector to Histogram
    */
  it should "Generate Correct Histogram From Dense Vector By Having Min Max Provided Where Using FeaturedHistogramFromDenseVector" in {
    val denseVector1 = new DenseVector[Double](Array(-2.30, -1.20, 0.90, 1.20, 3.95, 1.85, 2.70, 2.95, 3.40, 3.90, 4.20, 4.80, 5.70, 5.90))

    val histogramDouble1 = FeaturedHistogramFromDenseVector
      .getHistogram(denseVector1, binSize = Some(9.0), lowerBound = Some(1.20), upperBound = Some(4.8))
      .get

    val expectedBinEdge1 = DenseVector(Int.MinValue.toDouble +: Array(1.2, 1.6, 2.0, 2.4, 2.8, 3.2, 3.6, 4.0, 4.4, 4.8) :+ Int.MaxValue.toDouble)
    val expectedHist1 = DenseVector(Array(3.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 2.0, 1.0, 0.0, 3.0))

    ComparatorUtils.compareBreezeOutputVector(a = histogramDouble1.binEdges, b = expectedBinEdge1)
    ComparatorUtils.compareBreezeOutputVector(a = histogramDouble1.hist, b = expectedHist1)

    val denseVector2 = DenseVector.fill[Double](size = 13, v = -2.2)

    val histogramDouble2 = FeaturedHistogramFromDenseVector
      .getHistogram(denseVector2, binSize = Some(9.0), lowerBound = Some(-2.20), upperBound = Some(-2.20))
      .get

    val expectedBinEdge2 = DenseVector(Int.MinValue.toDouble +: Array(-2.21, -2.2) :+ Int.MaxValue.toDouble)
    val expectedHist2 = DenseVector(Array(0.0, 0.0, 21.0))

    ComparatorUtils.compareBreezeOutputVector(a = histogramDouble2.binEdges, b = expectedBinEdge2)
    ComparatorUtils.compareBreezeOutputVector(a = histogramDouble2.hist, b = expectedHist2)
  }

  /**
    * Testing Featured Histogram to MinMax Ranges
    */
  it should "Generate Correct MinMax Ranges From Featured Histogram" in {
    val reflexColumnEntryForCol0 = ColumnVectorEntry(columnName = "c0", columnValue = DenseVector(1.0, 10.0, 11.0))
    val reflexColumnEntryForCol1 = ColumnVectorEntry(columnName = "c1", columnValue = DenseVector(1.0, 21.0, -50.0))
    val reflexColumnEntryForCol2 = ColumnVectorEntry(columnName = "c2", columnValue = DenseVector(2.0, 4.9, -3.0))
    val reflexColumnEntryForCol3 = ColumnVectorEntry(columnName = "c3", columnValue = DenseVector(3.0, 5.0, 2.0))
    val reflexColumnEntryForCol4 = ColumnVectorEntry(columnName = "c4", columnValue = DenseVector(2.0, 2.0, 2.0))

    val denseMatrixRep: NamedMatrix = NamedMatrix(arrayOfVector = Array(reflexColumnEntryForCol0, reflexColumnEntryForCol1, reflexColumnEntryForCol2, reflexColumnEntryForCol3, reflexColumnEntryForCol4))

    val featuredHistogram = NamedMatrixToFeaturedHistogram(denseMatrixRep, None, None, None)

    val (minRanges, maxRanges, numberOfBins) = NamedMatrixToFeaturedHistogram.generateMinMaxRangeAndBinSizeFromFeaturedHist(featuredHistogram)

    val expectedMinBinRanges = mutable.Map("c0" -> 1.0, "c1" -> -50.0, "c2" -> -3.0, "c3" -> 2.0, "c4" -> 1.99)

    expectedMinBinRanges.foreach(eachElement => {
      minRanges.contains(eachElement._1) should be(true)
      minRanges(eachElement._1) == eachElement._2 should be(true)
    })

    val expectedMaxBinRanges = mutable.Map("c0" -> 11.0, "c1" -> 21.0, "c2" -> 4.9, "c3" -> 5.0, "c4" -> 2.01)

    expectedMaxBinRanges.foreach(eachElement => {
      maxRanges.contains(eachElement._1) should be(true)
      maxRanges(eachElement._1) == eachElement._2 should be(true)
    })

    val expectedNumberBinsInEachFeature = mutable.Map("c0" -> 10.0, "c1" -> 10.0, "c2" -> 10.0, "c3" -> 10.0, "c4" -> 1.0)

    expectedNumberBinsInEachFeature.foreach(eachElement => {
      numberOfBins.contains(eachElement._1) should be(true)
      numberOfBins(eachElement._1) == eachElement._2 should be(true)
    })
  }

  /**
    * Testing combine functionality of two bin edges
    */
  it should "Correctly Combine 2 Bins" in {
    val hist1 = new Histogram(histVector = null, binEdgesVector = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0, 9.0)))
    val hist2 = new Histogram(histVector = null, binEdgesVector = new DenseVector[Double](Array(2.0, 5.0, 8.0)))

    val combinedBinEdge = CombineBinEdges.reduce(hist1, hist2)

    val expectedCombinedBinEdge = new DenseVector[Double](Array(1.0, 2.0, 3.0, 5.0, 7.0, 8.0, 9.0))

    combinedBinEdge.equals(expectedCombinedBinEdge) should be(true)
  }

  /**
    * Testing combine functionality of two hist
    */
  it should "Correctly Combine Two Hist Details" in {
    val hist1 = new Histogram(histVector = new DenseVector[Double](Array(10.0, 6.0, 8.0, 8.0)), binEdgesVector = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0, 9.0)))
    val hist2 = new Histogram(histVector = new DenseVector[Double](Array(6.0, 6.0, 18.0, 15.0)), binEdgesVector = new DenseVector[Double](Array(2.0, 5.0, 8.0, 11.0, 14.0)))

    val combinedHist: Histogram = CombineHistograms.reduce(hist1, hist2)

    // hist is shrinked
    val expectedCombinedHistDetail = DenseVector[Double](22.0, 28.0, 12.0, 15.0)
    val expectedCombinedHistBin = DenseVector[Double](1.0, 5.0, 9.0, 11.0, 14.0)

    combinedHist.hist.equals(expectedCombinedHistDetail) should be(true)
    combinedHist.binEdges.equals(expectedCombinedHistBin) should be(true)
  }

  /**
    * Testing shrink functionality of hist
    */
  it should "Shrink Correctly Based On Shrink Size" in {
    val hist = new Histogram(histVector = new DenseVector[Double](Array(10.0, 5.0, 9.0, 8.0)), binEdgesVector = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0, 9.0)))

    val shrinkHistForSize4 = ShrinkHistogram.reduce(hist = hist, shrinkSize = 4)

    val expectedShrinkHistDetailForSize4 = DenseVector[Double](10.0, 14.0, 8.0)
    val expectedShrinkBinForSize4 = DenseVector[Double](1.0, 3.0, 7.0, 9.0)

    shrinkHistForSize4.hist.equals(expectedShrinkHistDetailForSize4) should be(true)
    shrinkHistForSize4.binEdges.equals(expectedShrinkBinForSize4) should be(true)

    val shrinkHistForSize3 = ShrinkHistogram.reduce(hist = hist, shrinkSize = 3)

    val expectedShrinkHistDetailForSize3 = DenseVector[Double](10.0, 22.0)
    val expectedShrinkBinForSize3 = DenseVector[Double](1.0, 3.0, 9.0)

    shrinkHistForSize3.hist.equals(expectedShrinkHistDetailForSize3) should be(true)
    shrinkHistForSize3.binEdges.equals(expectedShrinkBinForSize3) should be(true)
  }

  /**
    * Testing compare functionality of two histograms
    */
  it should "Correctly Generate Overlap Score For Two Hist" in {
    val inferringHist = new Histogram(histVector = new DenseVector[Double](Array(0.1, 0.2, 0.7)), binEdgesVector = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0)))
    val contenderHist = new Histogram(histVector = new DenseVector[Double](Array(0.1, 0.1, 0.6)), binEdgesVector = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0)))

    // contender has one value missing .. which is 0.2 (as flag is set, we can know that 0.2 is missing)
    // expectedScore = ((0.1 + 0.1 + 0.6 + 0.2) * (0.1 + 0.1 + 0.6 + 0.2) / (0.01 + 0.01 + 0.36 + 0.04)) * (((0.1 * 0.1) + (0.2 * 0.1) + (0.7 * 0.6) + (0.0 * 0.2) ) / ((0.1 + 0.2 + 0.7) * (0.1 + 0.1 + 0.6 + 0.2)))
    //               = 1.0714
    val score: Double = CompareTwoHistograms.compare(inferringHist, contenderHist, HistogramComparatorTypes.ProbabilityDistribution, addAdjustmentNormalizingEdge = true)

    val expectedScore = 1.00

    score should be(expectedScore +- 1e-2)
  }

  /**
    * Testing if two histograms are having hist values of two bin zeros, then score should be average of others bin only.
    */
  it should "Correctly Generate Overlap Score For Two Hist Having Zeros In Two Hist" in {
    val inferringHist = new Histogram(histVector = new DenseVector[Double](Array(30.0, 0.0, 20.0, 0.0)), binEdgesVector = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0, 9.0)))
    val contenderHist = new Histogram(histVector = new DenseVector[Double](Array(6.0, 0.0, 18.0, 0.0)), binEdgesVector = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0, 9.0)))

    // expectedScore = ((6.0 + 0.0 + 18.0 + 0.0) * (6.0 + 0.0 + 18.0 + 0.0) / (36.0 + 0.0 + 324.0 + 0.0)) * (((30.0 * 6.0) + (0.0 * 0.0) + (20.0 * 18.0) + (0.0 * 0.0) ) / ((30.0 + 0.0 + 20.0 + 0.0) * (6.0 + 0.0 + 18.0 + 0.0)))
    //               = 0.7200
    val score: Double = CompareTwoHistograms.compare(inferringHist, contenderHist, HistogramComparatorTypes.ProbabilityDistribution, addAdjustmentNormalizingEdge = false)

    val expectedScore = 0.7200

    score should be(expectedScore +- 1e-2)
  }

  /**
    * Testing compare functionality of two featured histograms
    */
  it should "Correctly Generate Overlap Score For Two Featured Hist" in {
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

    val scoreArray =
      CompareTwoFeaturedHistograms.compare(contenderfeaturedHistogram = map1OfFeatureIDAndHist, inferencefeaturedHistogram = map2OfFeatureIDAndHist, method = HistogramComparatorTypes.ProbabilityDistribution, addAdjustmentNormalizingEdge = false)

    val expectedScoreForFeature1 = 0.44008
    val expectedScoreForFeature2 = 0.29939

    scoreArray(featureID1) should be(expectedScoreForFeature1 +- 1e-2)
    scoreArray(featureID2) should be(expectedScoreForFeature2 +- 1e-2)
  }

  /**
    * Testing Histogram to String Conversion With Hist Normalization
    */
  it should "Testing Histogram to String Conversion With Hist Normalization" in {
    val featureID1 = "1"
    val feature1Hist = new DenseVector[Double](Array(10.0, 6.0, 4.0))
    val feature1BinEdge = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0))
    val hist1ForFeature1 = new Histogram(histVector = feature1Hist, binEdgesVector = feature1BinEdge)

    val featureID2 = "2"
    val feature2Hist = new DenseVector[Double](Array(15.0, 45.0))
    val feature2BinEdge = new DenseVector[Double](Array(4.0, 7.0, 10.0))
    val hist1ForFeature2 = new Histogram(histVector = feature2Hist, binEdgesVector = feature2BinEdge)

    val hist1NormJson = hist1ForFeature1.setEnableNormHist(true).toGraphString()
    val hist2NormJson = hist1ForFeature2.setEnableNormHist(true).toGraphString()

    val expectedStringRepresentationForFeature1 = "[{\"1.0 to 3.0\":0.5},{\"3.0 to 5.0\":0.3},{\"5.0 to 7.0\":0.2}]"
    hist1NormJson.equals(expectedStringRepresentationForFeature1) should be(true)

    val expectedStringRepresentationForFeature2 = "[{\"4.0 to 7.0\":0.25},{\"7.0 to 10.0\":0.75}]"
    hist2NormJson.equals(expectedStringRepresentationForFeature2) should be(true)
  }
  /**
    * Testing HistogramWrapper to String format functionality
    */
  it should "Test JSON String Conversion" in {
    val featureID1 = "1"
    val feature1Hist = new DenseVector[Double](Array(10.0, 6.0, 4.0))
    val feature1BinEdge = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0))
    val hist1ForFeature1 = new Histogram(histVector = feature1Hist, binEdgesVector = feature1BinEdge)

    val featureID2 = "2"
    val feature2Hist = new DenseVector[Double](Array(15.0, 93.0))
    val feature2BinEdge = new DenseVector[Double](Array(4.0, 7.0, 10.0))
    val hist1ForFeature2 = new Histogram(histVector = feature2Hist, binEdgesVector = feature2BinEdge)

    // map containing featureID and hist
    val mapOfFeatureIDAndHist = mutable.Map[String, Histogram]()
    mapOfFeatureIDAndHist += (featureID1 -> hist1ForFeature1)
    mapOfFeatureIDAndHist += (featureID2 -> hist1ForFeature2)

    val JSONRepresentationOfMapOfFeatureIDAndStringHist = HistogramWrapper(mapOfFeatureIDAndHist, false).toString

    val reconvertedMapOfFeatureIDAndHist = HistogramWrapper.fromString(JSONRepresentationOfMapOfFeatureIDAndStringHist)

    reconvertedMapOfFeatureIDAndHist(featureID1).hist.equals(feature1Hist) should be(true)
    reconvertedMapOfFeatureIDAndHist(featureID1).binEdges.equals(feature1BinEdge) should be(true)

    reconvertedMapOfFeatureIDAndHist(featureID2).hist.equals(feature2Hist) should be(true)
    reconvertedMapOfFeatureIDAndHist(featureID2).binEdges.equals(feature2BinEdge) should be(true)
  }

  /**
    * Testing WrapperHistogram toString and fromString functionality
    */
  it should "Test WrapperHistogram toString And fromString Functionality" in {
    val featureID1 = "1"

    val feature1Hist = new DenseVector[Double](Array(10.0, 6.0, 4.0))
    val sumOfF1Hist = sum(feature1Hist)
    val feature1NormHist = feature1Hist.map(x => x / sumOfF1Hist)
    val feature1BinEdge = new DenseVector[Double](Array(1.0, 3.0, 5.0, 7.0))

    val hist1ForFeature1 = new Histogram(histVector = feature1Hist, binEdgesVector = feature1BinEdge)

    val featureID2 = "2"

    val feature2Hist = new DenseVector[Double](Array(15.0, 105.0))
    val sumOfF2Hist = sum(feature2Hist)
    val feature2NormHist = feature2Hist.map(x => x / sumOfF2Hist)
    val feature2BinEdge = new DenseVector[Double](Array(4.0, 7.0, 10.0))
    val hist1ForFeature2 = new Histogram(histVector = feature2Hist, binEdgesVector = feature2BinEdge)

    val featureID3 = "3"

    val feature3Hist = new DenseVector[Double](Array(103.0, 697.0))
    val sumOfF3Hist = sum(feature3Hist)
    val feature3NormHist = feature3Hist.map(x => x / sumOfF3Hist)
    val feature3BinEdge = new DenseVector[Double](Array(9.0, 17.0, 25.0))
    val hist1ForFeature3 = new Histogram(histVector = feature3Hist, binEdgesVector = feature3BinEdge)

    // map containing featureID and hist
    val mapOfFeatureIDAndHist = mutable.Map[String, Histogram]()
    mapOfFeatureIDAndHist += (featureID1 -> hist1ForFeature1)
    mapOfFeatureIDAndHist += (featureID2 -> hist1ForFeature2)
    mapOfFeatureIDAndHist += (featureID3 -> hist1ForFeature3)

    val histogramWrapper = HistogramWrapper(mapOfFeatureIDAndHist)

    println("--- " + histogramWrapper.toString)
    val expectedHistWrapperString = "{\"2\":[{\"4.0 to 7.0\":0.125},{\"7.0 to 10.0\":0.875}]," +
      "\"1\":[{\"1.0 to 3.0\":0.5},{\"3.0 to 5.0\":0.3},{\"5.0 to 7.0\":0.2}]," +
      "\"3\":[{\"9.0 to 17.0\":0.12875},{\"17.0 to 25.0\":0.87125}]}"

    expectedHistWrapperString.sorted.equals(histogramWrapper.toString.sorted) should be(true)

    val reconvertedMapOfFeatureIDAndHist = HistogramWrapper.fromString(expectedHistWrapperString)

    ComparatorUtils
      .compareBreezeOutputVector(a = reconvertedMapOfFeatureIDAndHist(featureID1).hist,
        b = feature1NormHist) should be(true)
    reconvertedMapOfFeatureIDAndHist(featureID1).binEdges.equals(feature1BinEdge) should be(true)

    ComparatorUtils
      .compareBreezeOutputVector(a = reconvertedMapOfFeatureIDAndHist(featureID2).hist,
        b = feature2NormHist) should be(true)
    reconvertedMapOfFeatureIDAndHist(featureID2).binEdges.equals(feature2BinEdge) should be(true)


    ComparatorUtils
      .compareBreezeOutputVector(a = reconvertedMapOfFeatureIDAndHist(featureID3).hist,
        b = feature3NormHist) should be(true)
    reconvertedMapOfFeatureIDAndHist(featureID3).binEdges.equals(feature3BinEdge) should be(true)
  }

  // NamedVectors
  val testDataStreamOfNamedVectorForHeatMap: Seq[NamedVector] = Seq[NamedVector](
    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 1.0, OpType.CONTINUOUS),
      ColumnEntry(columnName = "B", columnValue = 10.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 2.0, OpType.CONTINUOUS),
      ColumnEntry(columnName = "B", columnValue = 100.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 0.0, OpType.CONTINUOUS),
      ColumnEntry(columnName = "B", columnValue = 100.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 4.0, OpType.CONTINUOUS),
      ColumnEntry(columnName = "B", columnValue = -10.0, OpType.CONTINUOUS))),

    NamedVector(Array[ColumnEntry](ColumnEntry(columnName = "A", columnValue = 5.0, OpType.CONTINUOUS),
      ColumnEntry(columnName = "B", columnValue = 50.0, OpType.CONTINUOUS)))
  )

  it should "Generate Correct Range From Spark RDD Of Named Vectors" in {
    val sparkSession = SparkSession.builder.
      master("local[*]").appName("Histograms API Test").getOrCreate()

    val sc = sparkSession.sparkContext

    val rddOfNV = sc.parallelize(testDataStreamOfNamedVectorForHeatMap, 4)

    val rangeByDefault = ContinuousHistogramForSpark
      .getMinMaxBinRange(rddOfNamedVectorNumerics = rddOfNV,
        minBinValueForEachFeatureForRef = None,
        maxBinValueForEachFeatureForRef = None)

    val minRangeByDefault = rangeByDefault._1.get
    val maxRangeByDefault = rangeByDefault._2.get

    val expectedMinRangeByDefault = Map("A" -> -1.7473, "B" -> -50.9951)
    val expectedMaxRangeByDefault = Map("A" -> 6.5473, "B" -> 150.9951)

    expectedMinRangeByDefault.foreach(eachTuple => {
      eachTuple._2 should be(minRangeByDefault(eachTuple._1) +- 1e-2)
    })
    expectedMaxRangeByDefault.foreach(eachTuple => {
      eachTuple._2 should be(maxRangeByDefault(eachTuple._1) +- 1e-2)
    })

    val rangeByMinMax = ContinuousHistogramForSpark
      .getMinMaxBinRange(rddOfNamedVectorNumerics = rddOfNV,
        minBinValueForEachFeatureForRef = None,
        maxBinValueForEachFeatureForRef = None,
        minMaxRangeType = MinMaxRangeForHistogramType.ByMinMax)

    val minRangeByMinMax = rangeByMinMax._1.get
    val maxRangeByMinMax = rangeByMinMax._2.get

    val expectedMinRangeByMinMax = Map("A" -> 0.0, "B" -> -10.0)
    val expectedMaxRangeByMinMax = Map("A" -> 5.0, "B" -> 100.0)

    expectedMinRangeByMinMax.foreach(eachTuple => {
      eachTuple._2 should be(minRangeByMinMax(eachTuple._1) +- 1e-2)
    })
    expectedMaxRangeByMinMax.foreach(eachTuple => {
      eachTuple._2 should be(maxRangeByMinMax(eachTuple._1) +- 1e-2)
    })

    sc.stop()
  }
}
