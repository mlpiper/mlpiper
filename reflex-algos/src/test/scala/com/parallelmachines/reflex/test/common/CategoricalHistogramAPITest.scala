package com.parallelmachines.reflex.test.common

import breeze.linalg.DenseVector
import org.junit.runner.RunWith
import org.mlpiper.datastructures.{ColumnVectorEntry, NamedMatrix}
import org.mlpiper.stat.histogram.HistogramComparatorTypes
import org.mlpiper.stat.histogram.categorical.{HistogramWrapper => CategoricalHistogramWrapper, _}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class CategoricalHistogramAPITest extends FlatSpec with Matchers {
  /**
    * Testing Dense Vector to Histogram
    */
  it should "Generate Correct Histogram From Dense Vector Where Using FeaturedHistogramFromDenseVector" in {
    val denseVector1 = new DenseVector[Double](Array(2.0, 2.0, 2.0))

    val categoricalHistogram1 = FeaturedHistogramFromDenseVector.getHistogram(denseVector1).get

    val expectedHist1 = Map("2.0" -> 3.0)

    categoricalHistogram1.getCategoricalCount == expectedHist1 should be(true)

    val denseVector2 = new DenseVector[Double](Array(1.0, 2.0, 3.0, 3.0, 4.0, 5.0, 1.0, 1.0, 2.0, 3.0, 4.0))

    val categoricalHistogram2 = FeaturedHistogramFromDenseVector.getHistogram(denseVector2).get

    val expectedHist2 = Map("1.0" -> 3.0, "2.0" -> 2.0, "3.0" -> 3.0, "4.0" -> 2.0, "5.0" -> 1.0)

    categoricalHistogram2.getCategoricalCount == expectedHist2 should be(true)
  }

  it should "Create Correct Histogram After Histogram Format" in {
    val hist = new Histogram(categoricalCounts = Map("1.0" -> 4.0, "2.0" -> 2.0, "3.0" -> 1.0))

    val formattedHist1 =
      HistogramFormatting.formatHistogram(hist,
        enableNormalization = true,
        setOfPredefinedCategories = Some(Set[String]("1.0", "2.0")))

    val expectedHistogram1 = Map[String, Double]("1.0" -> 0.5714, "2.0" -> 0.2857)

    ComparatorUtils.compareTwoMap(formattedHist1.getCategoricalCount, expectedHistogram1) should be(true)

    // negative test case
    val setOfNonExistedCategories: Option[Set[String]] = Some(Set[String]("4.0", "5.0", "2.0"))

    val formattedHist2 =
      HistogramFormatting.formatHistogram(hist,
        enableNormalization = true,
        setOfPredefinedCategories = setOfNonExistedCategories)

    val expectedHistogram2 = Map[String, Double]("2.0" -> 0.2857)

    ComparatorUtils.compareTwoMap(formattedHist2.getCategoricalCount, expectedHistogram2) should be(true)
  }

  it should "Create Correct Histogram After Histogram Format Where Predefined Categories Are Not There" in {
    val hist = new Histogram(categoricalCounts = Map("A" -> 4.0, "B" -> 2.0, "C" -> 1.0,
      "D" -> 3.0, "E" -> 5.0, "F" -> 7.0,
      "I" -> 8.0, "H" -> 9.0, "G" -> 5.0,
      "J" -> 10.0, "K" -> 12.0, "L" -> 11.0,
      "O" -> 14.0, "N" -> 13.0, "M" -> 15.0
    ))

    val formattedHist = HistogramFormatting.formatHistogram(hist,
      enableNormalization = true,
      setOfPredefinedCategories = None)

    // given setOfPredefinedCategories is none, histogram will contain top categories only!
    val expectedHistogram = Map[String, Double](
      "E" -> 0.042, "N" -> 0.1092, "J" -> 0.084, "F" -> 0.0588,
      "A" -> 0.0336, "M" -> 0.1261, "G" -> 0.042, "I" -> 0.0672,
      "L" -> 0.0924, "H" -> 0.0756, "K" -> 0.1008, "O" -> 0.1176, "D" -> 0.0252)

    ComparatorUtils.compareTwoMap(formattedHist.getCategoricalCount, expectedHistogram) should be(true)
  }


  /**
    * Testing Featured Histogram to From Named Matrix
    */
  it should "Testing Featured Histogram to From Named Matrix" in {
    val reflexColumnEntryForCol0 = ColumnVectorEntry(columnName = "c0", columnValue = DenseVector(1.0, 1.0, 1.0, 2.0, 2.0, 3.0, 1.0))
    val reflexColumnEntryForCol1 = ColumnVectorEntry(columnName = "c1", columnValue = DenseVector(1.0, 3.0, 4.0, 2.0, 5.0, 3.0, 1.0))

    val denseMatrixRep: NamedMatrix = NamedMatrix(arrayOfVector = Array(reflexColumnEntryForCol0, reflexColumnEntryForCol1))

    val featuredHistogram = NamedMatrixToFeaturedHistogram(denseMatrixRep)

    val expectedHistC0 = Map("1.0" -> 4.0, "2.0" -> 2.0, "3.0" -> 1.0)

    featuredHistogram("c0").getCategoricalCount == expectedHistC0 should be(true)

    val expectedHistC1 = Map("1.0" -> 2.0, "2.0" -> 1.0, "3.0" -> 2.0, "4.0" -> 1.0, "5.0" -> 1.0)

    featuredHistogram("c1").getCategoricalCount == expectedHistC1 should be(true)
  }

  /**
    * Testing combine functionality of two hist
    */
  it should "Correctly Combine Two Hist Details" in {
    val hist1 = new Histogram(categoricalCounts = Map("1.0" -> 4.0, "2.0" -> 2.0, "3.0" -> 1.0))
    val hist2 = new Histogram(categoricalCounts = Map("1.0" -> 1.0, "2.0" -> 4.0, "4.0" -> 2.0))

    val combinedHist: Histogram = CombineHistograms.reduce(hist1, hist2)

    val expectedHist = Map("1.0" -> 5.0, "2.0" -> 6.0, "3.0" -> 1.0, "4.0" -> 2.0)

    combinedHist.getCategoricalCount == expectedHist should be(true)
  }

  /**
    * Testing compare functionality of two histograms
    */
  it should "Correctly Generate Normalized Compare Score For Two Hist" in {
    val contenderHistogram = new Histogram(Map("1.0" -> 5.0, "2.0" -> 2.0, "3.0" -> 3.0))
    val inferringHistogram = new Histogram(Map("1.0" -> 45.0, "2.0" -> 25.0, "3.0" -> 30.0))

    val normScore: Double = CompareTwoHistograms.compare(contenderHistogram = contenderHistogram, inferringHistogram = inferringHistogram, HistogramComparatorTypes.ProbabilityDistribution, addAdjustmentNormalizingEdge = true)

    //                 square of(sum of contender histogram counts)      sum(contender histogram count * inferring histogram count)
    // expectedScore = -------------------------------------------- * -----------------------------------------------------------------
    //                   sum(square of contender histogram counts)     sum(contender histogram counts) * sum(inferring histogram counts)
    //               = ((5.0 + 2.0 + 3.0) * (5.0 + 2.0 + 3.0) / (25.0 + 4.0 + 9.0)) * (((5.0 * 45.0) + (2.0 * 25.0) + (3.0 * 30.0)) / ((5.0 + 2.0 + 3.0) * (45.0 + 25.0 + 30.0)))
    //               = 0.960527

    val expectedScore: Double = 0.960527

    normScore should be(expectedScore +- 1e-2)
  }

  /**
    * Testing compare functionality of two histograms For Two Normalized Hist
    */
  it should "Correctly Generate Normalized Compare Score For Two Normalized Hist" in {
    val contenderHistogram = new Histogram(Map("1.0" -> 0.5, "2.0" -> 0.20, "3.0" -> 0.20))
    val inferringHistogram = new Histogram(Map("1.0" -> 0.45, "2.0" -> 0.20, "3.0" -> 0.15))

    val normScore: Double =
      CompareTwoHistograms.compare(contenderHistogram = contenderHistogram,
        inferringHistogram = inferringHistogram,
        HistogramComparatorTypes.ProbabilityDistribution,
        addAdjustmentNormalizingEdge = true)
    // contender's other's category count = 1 - 0.5 - 0.2 - 0.2 = 0.1
    // inference's other's category count = 1 - 0.45 - 0.2 - 0.15 = 0.2
    //
    //                 square of(sum of contender histogram counts)      sum(contender histogram count * inferring histogram count)
    // expectedScore = -------------------------------------------- * -----------------------------------------------------------------
    //                   sum(square of contender histogram counts)     sum(contender histogram counts) * sum(inferring histogram counts)
    //               = ((1) * (1) / (0.25 + 0.04 + 0.04 + 0.01)) * (((0.5 * 0.45) + (0.2 * 0.2) + (0.2 * 0.15) + (0.1 * 0.2)) / ((1.0) * (1.0)))
    //               = 0.92647

    val expectedScore: Double = 0.92647

    normScore should be(expectedScore +- 1e-2)
  }

  /**
    * Testing compare functionality of two histograms
    */
  it should "Correctly Generate Normalized Compare Score For Two Hist Which Have Different Cates" in {
    val contenderHistogram = new Histogram(Map("1.0" -> 5.0, "2.0" -> 2.0, "3.0" -> 3.0))
    val inferringHistogram = new Histogram(Map("1.0" -> 45.0, "2.0" -> 25.0, "4.0" -> 30.0))

    val normScore: Double =
      CompareTwoHistograms.compare(contenderHistogram = contenderHistogram,
        inferringHistogram = inferringHistogram,
        HistogramComparatorTypes.ProbabilityDistribution,
        addAdjustmentNormalizingEdge = true)

    // expectedScore = ((5.0 + 2.0 + 3.0) * (5.0 + 2.0 + 3.0) / (25.0 + 4.0 + 9.0)) * (((5.0 * 45.0) + (2.0 * 25.0) + (3.0 * 0.0) + (0.0 * 30.0) ) / ((5.0 + 2.0 + 3.0) * (45.0 + 25.0 + 30.0)))
    //               = 0.723684

    val expectedScore: Double = 0.723684

    normScore should be(expectedScore +- 1e-2)
  }

  /**
    * Testing compare functionality of two featured histograms
    */
  it should "Correctly Generate Normalized Compare Score For Two Featured Histograms" in {
    val featureID1 = "0"
    val featureID2 = "1"

    val contenderHistogramForFeature1 = new Histogram(Map("1.0" -> 5.0, "2.0" -> 2.0, "3.0" -> 3.0))
    val inferringHistogramForFeature1 = new Histogram(Map("1.0" -> 45.0, "2.0" -> 25.0, "4.0" -> 30.0))

    val contenderHistogramForFeature2 = new Histogram(Map("1.0" -> 50.0, "2.0" -> 20.0, "3.0" -> 30.0))
    val inferringHistogramForFeature2 = new Histogram(Map("1.0" -> 4.5, "2.0" -> 2.5, "3.0" -> 3.0))

    // ref map containing featureID and hist
    val contenderFeatureIDAndHist = mutable.Map[String, Histogram](featureID1 -> contenderHistogramForFeature1, featureID2 -> contenderHistogramForFeature2)

    // contender map containing featureID and hist
    val inferringFeatureIDAndHist = mutable.Map[String, Histogram](featureID1 -> inferringHistogramForFeature1, featureID2 -> inferringHistogramForFeature2)

    val normScoreArray =
      CompareTwoFeaturedHistograms.compare(contenderFeatureHist = contenderFeatureIDAndHist,
        inferringFeatureHist = inferringFeatureIDAndHist,
        HistogramComparatorTypes.ProbabilityDistribution,
        addAdjustmentNormalizingEdge = true)

    val expectedScoreForFeature1: Double = 0.723684
    val expectedScoreForFeature2: Double = 0.960527

    normScoreArray(featureID1) should be(expectedScoreForFeature1 +- 1e-2)
    normScoreArray(featureID2) should be(expectedScoreForFeature2 +- 1e-2)
  }


  /**
    * Testing Histogram to String format functionality
    */
  it should "Test Categorical String Conversion" in {
    val histForFeature1 = new Histogram(categoricalCounts = Map("1.0" -> 4.0, "2.0" -> 2.0, "3.0" -> 1.0))
    val histForFeature2 = new Histogram(categoricalCounts = Map("1.0" -> 1.0, "2.0" -> 4.0, "4.0" -> 2.0))

    val hist1Json = histForFeature1.toGraphString()
    val hist2Json = histForFeature2.toGraphString()

    val expectedStringRepresentationForFeature1 = "[{\"1.0\":4.0},{\"2.0\":2.0},{\"3.0\":1.0}]"
    expectedStringRepresentationForFeature1.equals(hist1Json) should be(true)

    val expectedStringRepresentationForFeature2 = "[{\"1.0\":1.0},{\"2.0\":4.0},{\"4.0\":2.0}]"
    expectedStringRepresentationForFeature2.equals(hist2Json) should be(true)
  }

  /**
    * Testing WrapperHistogram toString and fromString functionality
    */
  //TODO: fix normalization
  it should "Test WrapperHistogram toString And fromString Functionality" in {
    val featureID1 = "1"

    val feature1Hist = Map("1.0" -> 4.0, "2.0" -> 2.0, "3.0" -> 4.0)

    val histForFeature1 = new Histogram(feature1Hist)

    val featureID2 = "2"

    val feature2Hist = Map("1.0" -> 5.0, "2.0" -> 3.0, "4.0" -> 2.0)

    val histForFeature2 = new Histogram(feature2Hist)

    // map containing featureID and hist
    val mapOfFeatureIDAndHist = mutable.Map[String, Histogram]()
    mapOfFeatureIDAndHist += (featureID1 -> histForFeature1)
    mapOfFeatureIDAndHist += (featureID2 -> histForFeature2)

    val histogramWrapper = CategoricalHistogramWrapper(mapOfFeatureIDAndHist)

    val expectedHistWrapperString = "{\"2\":[{\"1.0\":5.0},{\"2.0\":3.0},{\"4.0\":2.0}],\"1\":[{\"1.0\":4.0},{\"2.0\":2.0},{\"3.0\":4.0}]}"

    expectedHistWrapperString.equals(histogramWrapper.toString) should be(true)

    val reconvertedMapOfFeatureIDAndHist = CategoricalHistogramWrapper.fromString(expectedHistWrapperString)
    reconvertedMapOfFeatureIDAndHist(featureID1).getCategoricalCount == feature1Hist should be(true)
    reconvertedMapOfFeatureIDAndHist(featureID2).getCategoricalCount == feature2Hist should be(true)
  }
}
