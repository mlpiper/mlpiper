package com.parallelmachines.reflex.test.common

import breeze.linalg.{DenseMatrix, DenseVector => BreezeDenseVector}
import org.junit.runner.RunWith
import org.mlpiper.mlops.JSONData
import org.mlpiper.stats.AccumValueStringCreator
import org.mlpiper.utils.ParsingUtils
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.{immutable, mutable}

@RunWith(classOf[JUnitRunner])
class AccumValueStringCreatorTest extends FlatSpec with Matchers {

  it should "Correctly Create String Representation Of Type Scalar" in {
    val intV = 5
    val expectedIntRep = "{\"0\":5}"

    AccumValueStringCreator.toString(intV).sorted.equals(expectedIntRep.sorted) should be(true)

    val floatV = 5f
    val expectedFloatRep = "{\"0\":5.0}"

    AccumValueStringCreator.toString(floatV).sorted.equals(expectedFloatRep.sorted) should be(true)

    val longV = 5L
    val expectedLongRep = "{\"0\":5}"

    AccumValueStringCreator.toString(longV).sorted.equals(expectedLongRep.sorted) should be(true)

    val doubleV = 5.0
    val expectedDoubleRep = "{\"0\":5.0}"

    AccumValueStringCreator.toString(doubleV).sorted.equals(expectedDoubleRep.sorted) should be(true)
  }

  it should "Correctly Create String Representation Of Type Text" in {
    val stringV = "Accum"
    val expectedStringRep = "{\"0\":\"Accum\"}"

    AccumValueStringCreator.toString(stringV).sorted.equals(expectedStringRep.sorted) should be(true)
  }

  it should "Correctly Create String Representation Of Type Matrix" in {
    val matrixDouble = DenseMatrix(BreezeDenseVector(1.0, 2.0), BreezeDenseVector(3.0, 4.0), BreezeDenseVector(5.0, 6.0))

    val expectedMatrixDoubleRep =
      "{\"0\":{\"0\":\"1.0\",\"1\":\"2.0\"},\"1\":{\"0\":\"3.0\",\"1\":\"4.0\"},\"2\":{\"0\":\"5.0\",\"1\":\"6.0\"}}"

    AccumValueStringCreator.toString(matrixDouble).sorted.equals(expectedMatrixDoubleRep.sorted) should be(true)
  }

  it should "Correctly Create String Representation Matrix" in {
    val matrix = DenseMatrix(BreezeDenseVector(1.0, 2.0), BreezeDenseVector(3.0, 4.0), BreezeDenseVector(5.0, 6.0))
    val rowLabel = Array("row1", "row2", "row3")
    val colLabel = Array("col1", "col2")

    val calculatedJSON = ParsingUtils.breezeDenseMatrixToJsonMap(matrix = matrix, rowLabel = Some(rowLabel), colLabel = Some(colLabel))

    val expectedJSON =
      "{\"row1\":{\"col1\":\"1.0\",\"col2\":\"2.0\"},\"row2\":{\"col1\":\"3.0\",\"col2\":\"4.0\"},\"row3\":{\"col1\":\"5.0\",\"col2\":\"6.0\"}}"

    calculatedJSON.sorted.equals(expectedJSON.sorted) should be(true)
  }

  it should "Correctly Create String Representation Of Type Map" in {
    val map = Map(1 -> "2", 3 -> "4")
    val expecteMapRep = "{\"1\":\"2\",\"3\":\"4\"}"
    AccumValueStringCreator.toString(map).sorted.equals(expecteMapRep.sorted) should be(true)
  }

  it should "Correctly Create String Representation Of Type Vector" in {
    val listInt: immutable.List[Int] = immutable.List(1, 2, 3, 4)
    val expecteListRep = "{\"0\":1,\"1\":2,\"2\":3,\"3\":4}"
    AccumValueStringCreator.toString(listInt).sorted.equals(expecteListRep.sorted) should be(true)

    val listDouble: immutable.List[Double] = immutable.List(1.0, 2.0, 3.90, 4.09)
    val expecteListDRep = "{\"0\":1.0,\"1\":2.0,\"2\":3.9,\"3\":4.09}"
    AccumValueStringCreator.toString(listDouble).sorted.equals(expecteListDRep.sorted) should be(true)

    val mutListInt: mutable.ListBuffer[Int] = mutable.ListBuffer(1, 2, 3, 4)
    val expecteMutListRep = "{\"0\":1,\"1\":2,\"2\":3,\"3\":4}"
    AccumValueStringCreator.toString(mutListInt).sorted.equals(expecteMutListRep.sorted) should be(true)

    val arrayInt: Array[Int] = Array(1, 2, 3, 4)
    val expecteArrayRep = "{\"0\":1,\"1\":2,\"2\":3,\"3\":4}"
    AccumValueStringCreator.toString(arrayInt).sorted.equals(expecteArrayRep.sorted) should be(true)

    val bdvDouble: BreezeDenseVector[Double] = BreezeDenseVector(Array(1.0, 2.0, 3.0, 4.0))
    val expecteBDVRep = "{\"0\":1.0,\"1\":2.0,\"2\":3.0,\"3\":4.0}"
    AccumValueStringCreator.toString(bdvDouble).sorted.equals(expecteBDVRep.sorted) should be(true)
  }

  it should "Correctly create String representation of Type JSONStr" in {
    val directJson = "{\"Starting Quarter for training\": {\"Date\": \"2001Q1\"}, \"Endning Quarter for training\": {\"Date\": \"2014Q3\"}}"
    val jsonData = JSONData(directJson)
    AccumValueStringCreator.toString(jsonData).sorted.equals(directJson.sorted)
  }

}
