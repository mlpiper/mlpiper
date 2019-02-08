package com.parallelmachines.reflex.test.common

import breeze.linalg.DenseMatrix
import org.apache.flink.streaming.test.exampleScalaPrograms.clustering.ComparatorUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.mlpiper.stat.algos.ClassificationEvaluation
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class ClassificationEvaluationTest extends FlatSpec with Matchers {
  /**
    * Generate Correct Evaluation Matrix For Multiclass Evaluation
    */
  it should "Generate Correct Evaluation Matrix For Multiclass Evaluation" in {
    val predictedLabel = Array(1.0, 0.0, 2.0, 1.0, 0.0, 2.0, 1.0, 1.0, 0.0)
    val actualLabel = Array(1.0, 2.0, 2.0, 0.0, 2.0, 1.0, 1.0, 2.0, 0.0)
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val rdd = spark.sparkContext.parallelize(predictedLabel.zip(actualLabel))
    val df: DataFrame = spark.createDataFrame(rdd)

    val evaluationStatistics = ClassificationEvaluation
      .generateStats(
        dataFrame = df,
        labelTransformationDetails = None
      )

    val expectedConfusionMatrix = DenseMatrix((1.0, 1.0, 0.0, 0.5),
      (0.0, 2.0, 1.0, 0.3333),
      (2.0, 1.0, 1.0, 0.75),
      (3.0, 4.0, 2.0, 0.5555))
    val calculatedConfusionMatrix = evaluationStatistics.confusionMatrix.get.matrix

    ComparatorUtils.compareBreezeOutputMatrix(expectedConfusionMatrix, calculatedConfusionMatrix) should be(true)

    val calculatedAccuracy = evaluationStatistics.accuracy.get
    val expectedAccuracy = 0.4440
    calculatedAccuracy should be(expectedAccuracy +- 0.01)

    val calculatedWeightedPrecision = evaluationStatistics.weightedPrecision.get
    val expectedWeightedPrecision = 0.4630
    calculatedWeightedPrecision should be(expectedWeightedPrecision +- 0.01)

    val calculatedWeightedRecall = evaluationStatistics.weightedRecall.get
    val expectedWeightedRecall = 0.4440
    calculatedWeightedRecall should be(expectedWeightedRecall +- 0.01)

    val calculatedRecallPerClass = evaluationStatistics.recallPerclass.get.recallPerClassMap
    val expectedRecallPerClass = mutable.Map[String, Double]("0.0" -> 0.5, "1.0" -> 0.6666, "2.0" -> 0.25)

    for (eachKey <- calculatedRecallPerClass.keys) {
      calculatedRecallPerClass(eachKey) should be(expectedRecallPerClass(eachKey) +- 0.01)
    }

    val calculatedPrecisionPerClass = evaluationStatistics.precisionPerclass.get.precisionPerClassMap
    val expectedPrecisionPerClass = mutable.Map[String, Double]("2.0" -> 0.5, "1.0" -> 0.5, "0.0" -> 0.3333)

    for (eachKey <- calculatedPrecisionPerClass.keys) {
      calculatedPrecisionPerClass(eachKey) should be(expectedPrecisionPerClass(eachKey) +- 0.01)
    }
  }

  /**
    * Generate Correct Evaluation Matrix For Binaryclass Evaluation
    */
  it should "Generate Correct Evaluation Matrix For Binaryclass Evaluation" in {
    val predictedLabel = Array(1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0)
    val actualLabel = Array(1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0)

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val predictedAndActualLabelRDD = spark.sparkContext.parallelize(predictedLabel.zip(actualLabel))
    val df: DataFrame = spark.createDataFrame(predictedAndActualLabelRDD)

    val evaluationStatistics = ClassificationEvaluation
      .generateStats(
        dataFrame = df,
        labelTransformationDetails = None
      )

    val expectedConfusionMatrix = DenseMatrix((1.0, 3.0, 0.75), (2.0, 3.0, 0.4), (3.0, 6.0, 0.5555))

    val calculatedConfusionMatrix = evaluationStatistics.confusionMatrix.get.matrix

    ComparatorUtils.compareBreezeOutputMatrix(expectedConfusionMatrix, calculatedConfusionMatrix) should be(true)

    val calculatedAccuracy = evaluationStatistics.accuracy.get
    val expectedAccuracy = 0.4440
    calculatedAccuracy should be(expectedAccuracy +- 0.01)

    val calculatedWeightedPrecision = evaluationStatistics.weightedPrecision.get
    val expectedWeightedPrecision = 0.4260
    calculatedWeightedPrecision should be(expectedWeightedPrecision +- 0.01)

    val calculatedWeightedRecall = evaluationStatistics.weightedRecall.get
    val expectedWeightedRecall = 0.4440
    calculatedWeightedRecall should be(expectedWeightedRecall +- 0.01)

    val calculatedRecallPerClass = evaluationStatistics.recallPerclass.get.recallPerClassMap
    val expectedRecallPerClass = mutable.Map[String, Double]("1.0" -> 0.6, "0.0" -> 0.25)

    for (eachKey <- calculatedRecallPerClass.keys) {
      calculatedRecallPerClass(eachKey) should be(expectedRecallPerClass(eachKey) +- 0.01)
    }

    val calculatedPrecisionPerClass = evaluationStatistics.precisionPerclass.get.precisionPerClassMap
    val expectedPrecisionPerClass = mutable.Map[String, Double]("1.0" -> 0.5, "0.0" -> 0.3333)

    for (eachKey <- calculatedPrecisionPerClass.keys) {
      calculatedPrecisionPerClass(eachKey) should be(expectedPrecisionPerClass(eachKey) +- 0.01)
    }
  }
}
