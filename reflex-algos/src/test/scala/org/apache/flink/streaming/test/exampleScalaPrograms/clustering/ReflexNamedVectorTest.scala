package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import org.apache.flink.streaming.scala.examples.clustering.math.{ReflexColumnEntry, ReflexNamedVector}
import com.parallelmachines.reflex.common.enums.OpType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class ReflexNamedVectorTest extends FlatSpec with Matchers {
  /**
    * Testing Column Entry Which Will Have Numbers Only
    */
  it should "Generate Correct Named Vector Which Contains Only Numbers" in {
    val namedVector = ReflexNamedVector(Array(
      ReflexColumnEntry("Number1D", 0.100, OpType.CONTINUOUS),
      ReflexColumnEntry("Number2L", 100L, OpType.CONTINUOUS),
      ReflexColumnEntry("Number3I", 100, OpType.CONTINUOUS),
      ReflexColumnEntry("String1", "String", OpType.CONTINUOUS),
      ReflexColumnEntry("Byte", 2.0.toByte, OpType.CONTINUOUS)
    ), None, None)

    val expectedNamedVector = ReflexNamedVector(Array(
      ReflexColumnEntry("Number1D", 0.100, OpType.CONTINUOUS),
      ReflexColumnEntry("Number2L", 100L, OpType.CONTINUOUS),
      ReflexColumnEntry("Number3I", 100, OpType.CONTINUOUS),
      ReflexColumnEntry("Byte", 2.0, OpType.CONTINUOUS)
    ), None, None)

    namedVector.toContinuousNamedVector().compare(expectedNamedVector) should be(true)
  }

  /**
    * Testing Column Entry Which Will Have Numbers Only With Dropping Nas
    */
  it should "Generate Correct Named Vector Which Contains Only Numbers With Dropping Nas" in {
    val namedVector = ReflexNamedVector(Array(
      ReflexColumnEntry("Number1D", 0.100, OpType.CONTINUOUS),
      ReflexColumnEntry("Number2L", 100L, OpType.CONTINUOUS),
      ReflexColumnEntry("Number3NaN", Double.NaN, OpType.CONTINUOUS),
      ReflexColumnEntry("String1", "String", OpType.CONTINUOUS),
      ReflexColumnEntry("Byte", 2.0.toByte, OpType.CONTINUOUS),
      ReflexColumnEntry("null", null, OpType.CONTINUOUS)
    ), None, None)

    val expectedNamedVector = ReflexNamedVector(Array(
      ReflexColumnEntry("Number1D", 0.100, OpType.CONTINUOUS),
      ReflexColumnEntry("Number2L", 100L, OpType.CONTINUOUS),
      ReflexColumnEntry("Byte", 2.0, OpType.CONTINUOUS)
    ), None, None)

    namedVector.toContinuousNamedVector(dropNa = true).compare(expectedNamedVector) should be(true)
  }

  /**
    * Testing Column Entry Which Will Have Categorical Values Only With Dropping Nas
    */
  it should "Generate Column Entry Which Will Have Categorical Values Only With Dropping Nas" in {
    val namedVector = ReflexNamedVector(Array(
      ReflexColumnEntry("String1", "0.100", OpType.CATEGORICAL),
      ReflexColumnEntry("String2", "Hi", OpType.CATEGORICAL),
      ReflexColumnEntry("String3", null, OpType.CATEGORICAL),
      ReflexColumnEntry("String1", None, OpType.CONTINUOUS)
    ), None, None)

    val expectedNamedVector = ReflexNamedVector(Array(
      ReflexColumnEntry("String1", "0.100", OpType.CATEGORICAL),
      ReflexColumnEntry("String2", "Hi", OpType.CATEGORICAL)
    ), None, None)

    namedVector.toCategoricalNamedVector(dropNa = true).compare(expectedNamedVector) should be(true)
  }

  /**
    * Testing Column Entry Which Will Have Only Selected Cols
    */
  it should "Generate Correct Named Vector Which Contains Only Selected Columns" in {
    val namedVector = ReflexNamedVector(Array(
      ReflexColumnEntry("A", 0.100, OpType.CONTINUOUS),
      ReflexColumnEntry("B", 0.22, OpType.CONTINUOUS),
      ReflexColumnEntry("C", 2.0, OpType.CONTINUOUS),
      ReflexColumnEntry("D", 3.0, OpType.CONTINUOUS),
      ReflexColumnEntry("E", 2.0, OpType.CONTINUOUS)
    ), None, None)

    val expectedNamedVector = ReflexNamedVector(Array(
      ReflexColumnEntry("A", 0.100, OpType.CONTINUOUS),
      ReflexColumnEntry("C", 2.0, OpType.CONTINUOUS),
      ReflexColumnEntry("E", 2.0, OpType.CONTINUOUS)
    ), None, None)

    val setOfSelectCols = Set[String]("A", "E", "C")

    namedVector.selectCols(setOfSelectCols).compare(expectedNamedVector) should be(true)
  }

  /**
    * Testing Named Vector Should Contain Only Categorical / Continuous Entries Based On Function Applied
    */
  it should "Generate Only Categorical / Continuous Entries Based On Function Applied" in {
    val namedVector = ReflexNamedVector(Array(
      ReflexColumnEntry("A", 0.100, OpType.CONTINUOUS),
      ReflexColumnEntry("B", 0.22, OpType.CONTINUOUS),
      ReflexColumnEntry("C", 2.0, OpType.CATEGORICAL),
      ReflexColumnEntry("D", 3.0, OpType.CATEGORICAL),
      ReflexColumnEntry("E", 2.0, OpType.CATEGORICAL),
      ReflexColumnEntry("G", 0.22, OpType.CONTINUOUS)
    ), None, None)

    val expectedContinuousNamedVector = ReflexNamedVector(Array(
      ReflexColumnEntry("A", 0.100, OpType.CONTINUOUS),
      ReflexColumnEntry("B", 0.22, OpType.CONTINUOUS),
      ReflexColumnEntry("G", 0.22, OpType.CONTINUOUS)
    ), None, None)

    namedVector.toContinuousNamedVector().compare(expectedContinuousNamedVector) should be(true)

    val expectedCategoricalNamedVector = ReflexNamedVector(Array(
      ReflexColumnEntry("C", 2.0, OpType.CATEGORICAL),
      ReflexColumnEntry("D", 3.0, OpType.CATEGORICAL),
      ReflexColumnEntry("E", 2.0, OpType.CATEGORICAL)
    ), None, None)

    namedVector.toCategoricalNamedVector().compare(expectedCategoricalNamedVector) should be(true)
  }

  /**
    * Testing Stripping N/A to make an empty named vector so that the vector is omitted when creating histograms
    */
  it should "Strip N/A via toContinuousNamedVector" in {
    val expectedStrippedVectorList = List(
      ReflexNamedVector(Array(), None, None),
      ReflexNamedVector(Array(
        ReflexColumnEntry(
          "prediction",
          //JPMMLModelHandling.PredictionRowSchemaFieldName,
          1.233, OpType.CONTINUOUS)), None, None)
    )

    val vecList = List(
      ReflexNamedVector(Array(
        ReflexColumnEntry(
          "prediction",
          //JPMMLModelHandling.PredictionRowSchemaFieldName,
          "N/A", OpType.CONTINUOUS)),
        None, None),
      ReflexNamedVector(Array(
        ReflexColumnEntry(
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
