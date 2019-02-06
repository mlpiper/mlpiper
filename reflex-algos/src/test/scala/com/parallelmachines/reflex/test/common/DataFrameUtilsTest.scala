package com.parallelmachines.reflex.test.common

import breeze.linalg.DenseVector
import com.parallelmachines.reflex.pipeline.DataFrameUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class DataFrameUtilsTest extends FlatSpec with Matchers {
  /**
    * Combine Two DataFrames Correctly
    */
  it should "Correctly Combine Two DataFrames" in {
    val predictedLabel = Array[Double](1.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0)
    val actualLabel = Array[Double](1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0)

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val columnIndex = "columnIndex"

    val rddPrediction = spark.sparkContext.parallelize(predictedLabel)
    val predictedLabelColumnName = "predicted"
    val predictedLabelDF = spark.createDataFrame(rddPrediction.zipWithIndex()).toDF(predictedLabelColumnName, columnIndex).drop(columnIndex)

    val rddActual = spark.sparkContext.parallelize(actualLabel)
    val actualLabelColumnName = "actual"
    val actualLabelDF = spark.createDataFrame(rddActual.zipWithIndex()).toDF(actualLabelColumnName, columnIndex).drop(columnIndex)

    val combinedDataFrame = DataFrameUtils
      .mergeTwoDataFramesSingleColumn(
        dataFrame1 = predictedLabelDF,
        columnNameOf1 = predictedLabelColumnName,
        dataFrame2 = actualLabelDF,
        columnNameOf2 = actualLabelColumnName)

    val predictedAndActualLabelRDD = spark.sparkContext.parallelize(predictedLabel.zip(actualLabel))
    val expectedDataFrame: DataFrame = spark.createDataFrame(predictedAndActualLabelRDD).toDF(predictedLabelColumnName, actualLabelColumnName)

    val expectedSeq = expectedDataFrame.collect()
    var actualSeq = combinedDataFrame.collect()

    for (eachExpectedRow <- expectedSeq) {
      actualSeq.contains(eachExpectedRow) should be(true)
      actualSeq = actualSeq.toBuffer.-(eachExpectedRow).toArray
    }
  }

  "DataFrame to RDD[DenseVector[Double]] " should "valid" in {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val data = Seq(Row(1.1, 1.2), Row(2.1, 2.2), Row(3.1, 3.2))

    val schema = List(
      StructField("feature1", DoubleType, true),
      StructField("feature2", DoubleType, true)
    )

    val testDF = spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))
    val rddCollected = DataFrameUtils.toRDDofDenseVectorDouble(testDF, Some(0.0)).collect()

    assert(rddCollected.length == 3)
    assert(rddCollected(0) == DenseVector[Double](1.1, 1.2))
    assert(rddCollected(1) == DenseVector[Double](2.1, 2.2))
    assert(rddCollected(2) == DenseVector[Double](3.1, 3.2))
  }

  it should "Verify Categorical and Continuous data determination" in {
    val sparkSession = SparkSession.builder.
      master("local[*]").appName("Good Categorical DA").getOrCreate()

    val modelPath1: String = getClass.getResource("/dataanalysismissing.csv").getPath
    val df: DataFrame = sparkSession.read
      .format("csv")
      .option("header", "true")
      .load(modelPath1)

    val rddNV = DataFrameUtils.toRDDOfNamedVectorUsingSparkML(df, None, None)
    val rddNVCont = rddNV.map(_.toContinuousNamedVector())
    val rddNVCat = rddNV.map(_.toCategoricalNamedVector())

    val contCols = DataFrameUtils.fromRDDOfNamedVector(rddNVCont, sparkSession.sparkContext).columns
    val catCols = DataFrameUtils.fromRDDOfNamedVector(rddNVCat, sparkSession.sparkContext).columns

    val ansCatCols = Set("Missing::All", "Missing::Even", "Missing::Seq", "emptystrings")
    val ansContcols = Set("Missing::Float")
    assert(contCols.toSet.equals(ansContcols))
    assert(catCols.toSet.equals(ansCatCols))
  }
}
