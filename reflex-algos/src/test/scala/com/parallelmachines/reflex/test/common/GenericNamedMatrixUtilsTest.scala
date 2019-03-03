package com.parallelmachines.reflex.test.common

import breeze.linalg.DenseVector
import com.parallelmachines.reflex.common.enums.OpType
import com.parallelmachines.reflex.pipeline.DataFrameUtils
import com.parallelmachines.reflex.test.common.ComparatorUtils
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.mlpiper.datastructures.{ColumnEntry, NamedVector}
import org.mlpiper.utils.{GenericConstants, GenericNamedMatrixUtils, ParsingUtils}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class GenericNamedMatrixUtilsTest extends FlatSpec with Matchers {
  behavior of "Correct Pre-Processing Of Elements Based On Engine - Spark/Flink"

  "Iterator[NamedVector] to ReflexNamedMatrix; Filter empty vectors" should "be valid" in {
    val expectedFilteredMatrxString = "prediction : DenseVector(9.3434, 1.233)"
    val vecList = List(NamedVector(Array(), None, None),
      NamedVector(Array(
        ColumnEntry(
          "prediction", //JPMMLModelHandling.PredictionRowSchemaFieldName,
          9.3434, OpType.CONTINUOUS)),
        None, None),
      NamedVector(Array(
        ColumnEntry(
          "prediction", //JPMMLModelHandling.PredictionRowSchemaFieldName,
          1.233, OpType.CONTINUOUS)),
        None, None),
      NamedVector(Array(), None, None),
      NamedVector(Array(), None, None))

    val namedMatrix = GenericNamedMatrixUtils.iteratorOfNamedVectorToNamedMatrix(vecList.toIterator)
    namedMatrix.toString should be(expectedFilteredMatrxString)
  }

  "DataFrame to RDD[NamedVector]" should "be valid" in {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val data = Seq(Row(1.1), Row(2.1), Row(3.1), Row(4.1))

    val structField1 = StructField("feature1", DoubleType)
    val schema = List(
      structField1
    )

    val testDF = spark.createDataFrame(spark.sparkContext.parallelize(data, 2), StructType(schema))
    GenericConstants.MaximumCategoricalUniqueValue = 3

    var namedVec = DataFrameUtils.toRDDOfNamedVectorUsingSparkML(df = testDF, sparkMLModel = None, columnMap = None).collect()
    namedVec.foreach(vec => {
      vec.toCategoricalNamedVector().vector.length == 0 should be(true)
      vec.toContinuousNamedVector().vector.length == 1 should be(true)
    })

    GenericConstants.MaximumCategoricalUniqueValue = 25

    namedVec = DataFrameUtils.toRDDOfNamedVectorUsingSparkML(df = testDF, sparkMLModel = None, columnMap = None).collect()
    namedVec.foreach(vec => {
      vec.toCategoricalNamedVector().vector.length == 1 should be(true)
      vec.toContinuousNamedVector().vector.length == 0 should be(true)
    })
  }

  "DataFrame to RDD[NamedMatrix] " should "be valid" in {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val data = Seq(Row(1.1, 1.2), Row(2.1, 2.2), Row(3.1, 3.2), Row(4.1, 4.2))

    val structField1 = StructField("feature1", DoubleType)
    val structField2 = StructField("feature2", DoubleType)
    val schema = List(
      structField1,
      structField2
    )

    val testDF = spark.createDataFrame(spark.sparkContext.parallelize(data, 2), StructType(schema))

    val collectedResults = GenericNamedMatrixUtils.createReflexNamedMatrix(df = testDF, sparkMLModel = None).collect()

    val reflexColumnEntryForCol1_1 = ColumnEntry(columnName = structField1.name, columnValue = DenseVector(1.1, 2.1), OpType.CONTINUOUS)
    val reflexColumnEntryForCol2_1 = ColumnEntry(columnName = structField2.name, columnValue = DenseVector(1.2, 2.2), OpType.CONTINUOUS)

    val reflexColumnEntryForCol1_2 = ColumnEntry(columnName = structField1.name, columnValue = DenseVector(3.1, 4.1), OpType.CONTINUOUS)
    val reflexColumnEntryForCol2_2 = ColumnEntry(columnName = structField2.name, columnValue = DenseVector(3.2, 4.2), OpType.CONTINUOUS)

    collectedResults.foreach(eachResult => {
      val firstFieldEntry = eachResult
        .getColumnEntryFromColumnName(structField1.name)
        .get
        .columnValue.asInstanceOf[DenseVector[Double]]

      val field1Exists =
        ComparatorUtils.compareBreezeOutputVector(a = firstFieldEntry, b = reflexColumnEntryForCol1_1.columnValue.asInstanceOf[DenseVector[Double]]) ||
          ComparatorUtils.compareBreezeOutputVector(a = firstFieldEntry, b = reflexColumnEntryForCol1_2.columnValue.asInstanceOf[DenseVector[Double]])


      val secondFieldEntry = eachResult
        .getColumnEntryFromColumnName(structField2.name)
        .get
        .columnValue.asInstanceOf[DenseVector[Double]]

      val field2Exists =
        ComparatorUtils.compareBreezeOutputVector(a = secondFieldEntry, b = reflexColumnEntryForCol2_1.columnValue.asInstanceOf[DenseVector[Double]]) ||
          ComparatorUtils.compareBreezeOutputVector(a = secondFieldEntry, b = reflexColumnEntryForCol2_2.columnValue.asInstanceOf[DenseVector[Double]])

      field1Exists should be(true)
      field2Exists should be(true)
    })
  }

  "RDD[NamedVector] to RDD[NamedMatrix]" should "be valid" in {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val data = Seq(DenseVector(1.1, 1.2), DenseVector(2.1, 2.2), DenseVector(3.1, 3.2), DenseVector(4.1, 4.2))

    val testRDD = spark.sparkContext.parallelize(data, 2).map(ParsingUtils.denseVectorToNamedVector(_).get)

    val collectedResults = GenericNamedMatrixUtils
      .createReflexNamedMatrix(rddOfNamedVector = testRDD).collect()

    val reflexColumnEntryForCol1_1 = ColumnEntry(columnName = "c0", columnValue = DenseVector(1.1, 2.1), OpType.CONTINUOUS)
    val reflexColumnEntryForCol2_1 = ColumnEntry(columnName = "c1", columnValue = DenseVector(1.2, 2.2), OpType.CONTINUOUS)

    val reflexColumnEntryForCol1_2 = ColumnEntry(columnName = "c0", columnValue = DenseVector(3.1, 4.1), OpType.CONTINUOUS)
    val reflexColumnEntryForCol2_2 = ColumnEntry(columnName = "c1", columnValue = DenseVector(3.2, 4.2), OpType.CONTINUOUS)

    collectedResults.foreach(eachResult => {
      val firstFieldEntry = eachResult
        .getColumnEntryFromColumnName("c0")
        .get
        .columnValue.asInstanceOf[DenseVector[Double]]

      val field1Exists =
        ComparatorUtils.compareBreezeOutputVector(a = firstFieldEntry, b = reflexColumnEntryForCol1_1.columnValue.asInstanceOf[DenseVector[Double]]) ||
          ComparatorUtils.compareBreezeOutputVector(a = firstFieldEntry, b = reflexColumnEntryForCol1_2.columnValue.asInstanceOf[DenseVector[Double]])


      val secondFieldEntry = eachResult
        .getColumnEntryFromColumnName("c1")
        .get
        .columnValue.asInstanceOf[DenseVector[Double]]

      val field2Exists =
        ComparatorUtils.compareBreezeOutputVector(a = secondFieldEntry, b = reflexColumnEntryForCol2_1.columnValue.asInstanceOf[DenseVector[Double]]) ||
          ComparatorUtils.compareBreezeOutputVector(a = secondFieldEntry, b = reflexColumnEntryForCol2_2.columnValue.asInstanceOf[DenseVector[Double]])

      field1Exists should be(true)
      field2Exists should be(true)
    })
  }
}

