package com.parallelmachines.reflex.test.common

import breeze.linalg.DenseVector
import com.parallelmachines.reflex.common._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.mlpiper.stat.healthlib.{CategoricalHealth, HealthType}
import org.mlpiper.utils.{GenericNamedMatrixUtils, ParsingUtils}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class CategoricalHistogramTest extends FlatSpec with Matchers {
  /**
    * Generate Correct Categorical Histogram
    */
  it should "Generate Correct Categorical Histogram" in {
    val sparkSession = SparkSession.builder.
      master("local[*]").appName("Good Histograms").getOrCreate()

    val sequenceOfDenseVector: Seq[DenseVector[Double]] = Seq[DenseVector[Double]](
      DenseVector(0.0, 1.0, 2.0, 4.0),
      DenseVector(0.0, 1.0, 2.0, 5.0),
      DenseVector(1.0, 0.0, 1.0, 4.0),
      DenseVector(2.0, 1.0, 0.0, 3.0),
      DenseVector(2.0, 2.0, 0.0, 6.0)
    )
    val rddOfDenseVector: RDD[DenseVector[Double]] = sparkSession.sparkContext.parallelize(sequenceOfDenseVector, numSlices = 1)

    val rddOfNV = rddOfDenseVector.map(ParsingUtils.denseVectorToNamedVector(_).get)

    val rddOfNM = GenericNamedMatrixUtils.createReflexNamedMatrix(rddOfNV)

    val histogram = new CategoricalHealth(HealthType.CategoricalHistogramHealth.toString)
      .createHistogram(rddOfNamedMatrix = rddOfNM,
        enableAccumOutputOfHistograms = true,
        setOfPredefinedCategoriesForFeatures = None,
        sc = sparkSession.sparkContext,
        InfoType.InfoType.Health,
        modelId = null)

    val expectedCategoricalHistogramForC0 = Map[String, Double]("0.0" -> 0.4, "1.0" -> 0.2, "2.0" -> 0.4)

    histogram("c0").getCategoricalCount == expectedCategoricalHistogramForC0 should be(true)

    val expectedCategoricalHistogramForC1 = Map[String, Double]("0.0" -> 0.2, "1.0" -> 0.6, "2.0" -> 0.2)

    histogram("c1").getCategoricalCount == expectedCategoricalHistogramForC1 should be(true)

    val expectedCategoricalHistogramForC2 = Map[String, Double]("0.0" -> 0.4, "1.0" -> 0.2, "2.0" -> 0.4)

    histogram("c2").getCategoricalCount == expectedCategoricalHistogramForC2 should be(true)

    val expectedCategoricalHistogramForC3 = Map[String, Double]("3.0" -> 0.2, "4.0" -> 0.4, "5.0" -> 0.2, "6.0" -> 0.2)

    histogram("c3").getCategoricalCount == expectedCategoricalHistogramForC3 should be(true)
  }

  /**
    * Generate Correct Categorical Histogram With Higher Partitions
    */
  it should "Generate Correct Categorical Histogram With Higher Partition" in {
    val sparkSession = SparkSession.builder.
      master("local[*]").appName("Good Histograms").getOrCreate()

    val sequenceOfDenseVector: Seq[DenseVector[Double]] = Seq[DenseVector[Double]](
      DenseVector(0.0, 1.0, 2.0, 4.0),
      DenseVector(0.0, 1.0, 2.0, 5.0),
      DenseVector(1.0, 0.0, 1.0, 4.0),
      DenseVector(2.0, 1.0, 0.0, 3.0),
      DenseVector(2.0, 2.0, 0.0, 6.0)
    )
    val rddOfDenseVector: RDD[DenseVector[Double]] = sparkSession.sparkContext.parallelize(sequenceOfDenseVector, numSlices = 3)

    val rddOfNV = rddOfDenseVector.map(ParsingUtils.denseVectorToNamedVector(_).get)

    val rddOfNM = GenericNamedMatrixUtils.createReflexNamedMatrix(rddOfNV)

    val histogram = new CategoricalHealth(HealthType.CategoricalHistogramHealth.toString)
      .createHistogram(rddOfNamedMatrix = rddOfNM,
        enableAccumOutputOfHistograms = true,
        setOfPredefinedCategoriesForFeatures = None,
        sc = sparkSession.sparkContext,
        InfoType.InfoType.Health,
        modelId = null)

    val expectedCategoricalHistogramForC0 = Map[String, Double]("0.0" -> 0.4, "1.0" -> 0.2, "2.0" -> 0.4)

    histogram("c0").getCategoricalCount == expectedCategoricalHistogramForC0 should be(true)

    val expectedCategoricalHistogramForC1 = Map[String, Double]("0.0" -> 0.2, "1.0" -> 0.6, "2.0" -> 0.2)

    histogram("c1").getCategoricalCount == expectedCategoricalHistogramForC1 should be(true)

    val expectedCategoricalHistogramForC2 = Map[String, Double]("0.0" -> 0.4, "1.0" -> 0.2, "2.0" -> 0.4)

    histogram("c2").getCategoricalCount == expectedCategoricalHistogramForC2 should be(true)

    val expectedCategoricalHistogramForC3 = Map[String, Double]("3.0" -> 0.2, "4.0" -> 0.4, "5.0" -> 0.2, "6.0" -> 0.2)

    histogram("c3").getCategoricalCount == expectedCategoricalHistogramForC3 should be(true)
  }
}
