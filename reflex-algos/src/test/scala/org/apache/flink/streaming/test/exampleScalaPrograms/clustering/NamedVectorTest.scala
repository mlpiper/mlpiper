package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import com.parallelmachines.reflex.common.enums.OpType
import org.junit.runner.RunWith
import org.mlpiper.datastructures.{ColumnEntry, NamedVector}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class NamedVectorTest extends FlatSpec with Matchers {
  /**
    * Testing Column Entry Which Will Have Numbers Only
    */
  it should "Generate Correct Named Vector Which Contains Only Numbers" in {
    val namedVector = NamedVector(Array(
      ColumnEntry("Number1D", 0.100, OpType.CONTINUOUS),
      ColumnEntry("Number2L", 100L, OpType.CONTINUOUS),
      ColumnEntry("Number3I", 100, OpType.CONTINUOUS),
      ColumnEntry("String1", "String", OpType.CONTINUOUS),
      ColumnEntry("Byte", 2.0.toByte, OpType.CONTINUOUS)
    ), None, None)

    val expectedNamedVector = NamedVector(Array(
      ColumnEntry("Number1D", 0.100, OpType.CONTINUOUS),
      ColumnEntry("Number2L", 100L, OpType.CONTINUOUS),
      ColumnEntry("Number3I", 100, OpType.CONTINUOUS),
      ColumnEntry("Byte", 2.0, OpType.CONTINUOUS)
    ), None, None)

    namedVector.toContinuousNamedVector().compare(expectedNamedVector) should be(true)
  }

  /**
    * Testing Column Entry Which Will Have Numbers Only With Dropping Nas
    */
  it should "Generate Correct Named Vector Which Contains Only Numbers With Dropping Nas" in {
    val namedVector = NamedVector(Array(
      ColumnEntry("Number1D", 0.100, OpType.CONTINUOUS),
      ColumnEntry("Number2L", 100L, OpType.CONTINUOUS),
      ColumnEntry("Number3NaN", Double.NaN, OpType.CONTINUOUS),
      ColumnEntry("String1", "String", OpType.CONTINUOUS),
      ColumnEntry("Byte", 2.0.toByte, OpType.CONTINUOUS),
      ColumnEntry("null", null, OpType.CONTINUOUS)
    ), None, None)

    val expectedNamedVector = NamedVector(Array(
      ColumnEntry("Number1D", 0.100, OpType.CONTINUOUS),
      ColumnEntry("Number2L", 100L, OpType.CONTINUOUS),
      ColumnEntry("Byte", 2.0, OpType.CONTINUOUS)
    ), None, None)

    namedVector.toContinuousNamedVector(dropNa = true).compare(expectedNamedVector) should be(true)
  }

  /**
    * Testing Column Entry Which Will Have Categorical Values Only With Dropping Nas
    */
  it should "Generate Column Entry Which Will Have Categorical Values Only With Dropping Nas" in {
    val namedVector = NamedVector(Array(
      ColumnEntry("String1", "0.100", OpType.CATEGORICAL),
      ColumnEntry("String2", "Hi", OpType.CATEGORICAL),
      ColumnEntry("String3", null, OpType.CATEGORICAL),
      ColumnEntry("String1", None, OpType.CONTINUOUS)
    ), None, None)

    val expectedNamedVector = NamedVector(Array(
      ColumnEntry("String1", "0.100", OpType.CATEGORICAL),
      ColumnEntry("String2", "Hi", OpType.CATEGORICAL)
    ), None, None)

    namedVector.toCategoricalNamedVector(dropNa = true).compare(expectedNamedVector) should be(true)
  }

  /**
    * Testing Column Entry Which Will Have Only Selected Cols
    */
  it should "Generate Correct Named Vector Which Contains Only Selected Columns" in {
    val namedVector = NamedVector(Array(
      ColumnEntry("A", 0.100, OpType.CONTINUOUS),
      ColumnEntry("B", 0.22, OpType.CONTINUOUS),
      ColumnEntry("C", 2.0, OpType.CONTINUOUS),
      ColumnEntry("D", 3.0, OpType.CONTINUOUS),
      ColumnEntry("E", 2.0, OpType.CONTINUOUS)
    ), None, None)

    val expectedNamedVector = NamedVector(Array(
      ColumnEntry("A", 0.100, OpType.CONTINUOUS),
      ColumnEntry("C", 2.0, OpType.CONTINUOUS),
      ColumnEntry("E", 2.0, OpType.CONTINUOUS)
    ), None, None)

    val setOfSelectCols = Set[String]("A", "E", "C")

    namedVector.selectCols(setOfSelectCols).compare(expectedNamedVector) should be(true)
  }

  /**
    * Testing Named Vector Should Contain Only Categorical / Continuous Entries Based On Function Applied
    */
  it should "Generate Only Categorical / Continuous Entries Based On Function Applied" in {
    val namedVector = NamedVector(Array(
      ColumnEntry("A", 0.100, OpType.CONTINUOUS),
      ColumnEntry("B", 0.22, OpType.CONTINUOUS),
      ColumnEntry("C", 2.0, OpType.CATEGORICAL),
      ColumnEntry("D", 3.0, OpType.CATEGORICAL),
      ColumnEntry("E", 2.0, OpType.CATEGORICAL),
      ColumnEntry("G", 0.22, OpType.CONTINUOUS)
    ), None, None)

    val expectedContinuousNamedVector = NamedVector(Array(
      ColumnEntry("A", 0.100, OpType.CONTINUOUS),
      ColumnEntry("B", 0.22, OpType.CONTINUOUS),
      ColumnEntry("G", 0.22, OpType.CONTINUOUS)
    ), None, None)

    namedVector.toContinuousNamedVector().compare(expectedContinuousNamedVector) should be(true)

    val expectedCategoricalNamedVector = NamedVector(Array(
      ColumnEntry("C", 2.0, OpType.CATEGORICAL),
      ColumnEntry("D", 3.0, OpType.CATEGORICAL),
      ColumnEntry("E", 2.0, OpType.CATEGORICAL)
    ), None, None)

    namedVector.toCategoricalNamedVector().compare(expectedCategoricalNamedVector) should be(true)
  }

  /**
    * Testing Stripping N/A to make an empty named vector so that the vector is omitted when creating histograms
    */
  it should "Strip N/A via toContinuousNamedVector" in {
    val expectedStrippedVectorList = List(
      NamedVector(Array(), None, None),
      NamedVector(Array(
        ColumnEntry(
          "prediction",
          //JPMMLModelHandling.PredictionRowSchemaFieldName,
          1.233, OpType.CONTINUOUS)), None, None)
    )

    val vecList = List(
      NamedVector(Array(
        ColumnEntry(
          "prediction",
          //JPMMLModelHandling.PredictionRowSchemaFieldName,
          "N/A", OpType.CONTINUOUS)),
        None, None),
      NamedVector(Array(
        ColumnEntry(
          "prediction",
          //JPMMLModelHandling.PredictionRowSchemaFieldName,
          1.233, OpType.CONTINUOUS)),
        None, None)
    )

    var i = 0
    vecList.map(_.toContinuousNamedVector()).foreach(namedVector => {
      (i >= expectedStrippedVectorList.length) should be(false)
      namedVector.compare(expectedStrippedVectorList(i)) should be(true)
      i += 1
    })
  }
}
