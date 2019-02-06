package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.flink.streaming.scala.examples.common.stats.AccumData
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class AccumulatorInfoTest extends FlatSpec with Matchers {
  behavior of "Accumulator Correct DataType Generation"

  it should "Checking Line graph Functionality" in {
    val expectedStringForNumScalar = AccumData.withName("LINEGRAPH")

    val intValue: Int = 1
    AccumData.getGraphType(data = intValue).equals(expectedStringForNumScalar) should be(true)

    val floatValue: Float = 1.0f
    AccumData.getGraphType(data = floatValue).equals(expectedStringForNumScalar) should be(true)

    val longValue: Long = 1L
    AccumData.getGraphType(data = longValue).equals(expectedStringForNumScalar) should be(true)

    val doubleValue: Double = 1.0
    AccumData.getGraphType(data = doubleValue).equals(expectedStringForNumScalar) should be(true)
  }

  it should "Checking Text Scalar Functionality" in {
    val expectedStringForTextScalar = AccumData.withName("KPI")

    val stringValue: String = "String"
    AccumData.getGraphType(data = stringValue).equals(expectedStringForTextScalar) should be(true)
  }

  it should "Checking Map Functionality" in {
    val expectedStringForMap = AccumData.withName("MULTILINEGRAPH")

    val immutableMapIntInt: Map[Int, Int] = Map(1 -> 2, 2 -> 3)
    AccumData.getGraphType(data = immutableMapIntInt).equals(expectedStringForMap) should be(true)

    val immutableMapIntString: Map[Int, String] = Map(1 -> "2", 2 -> "3")
    AccumData.getGraphType(data = immutableMapIntString).equals(expectedStringForMap) should be(true)

    val immutableMapStringString: Map[String, String] = Map("1" -> "2", "2" -> "3")
    AccumData.getGraphType(data = immutableMapStringString).equals(expectedStringForMap) should be(true)

    val immutableMapStringDV: Map[String, DenseVector[Double]] = Map("1" -> DenseVector(1.0, 2.0), "2" -> DenseVector(2.0, 3.0))
    AccumData.getGraphType(data = immutableMapStringDV).equals(expectedStringForMap) should be(true)

    val mutableMapIntInt: mutable.Map[Int, Int] = mutable.Map(1 -> 2, 2 -> 3)
    AccumData.getGraphType(data = mutableMapIntInt).equals(expectedStringForMap) should be(true)

    val mutableMapIntString: mutable.Map[Int, String] = mutable.Map(1 -> "2", 2 -> "3")
    AccumData.getGraphType(data = mutableMapIntString).equals(expectedStringForMap) should be(true)

    val mutableMapStringString: mutable.Map[String, String] = mutable.Map("1" -> "2", "2" -> "3")
    AccumData.getGraphType(data = mutableMapStringString).equals(expectedStringForMap) should be(true)

    val mutableMapStringDV: mutable.Map[String, DenseVector[Double]] = mutable.Map("1" -> DenseVector(1.0, 2.0), "2" -> DenseVector(2.0, 3.0))
    AccumData.getGraphType(data = mutableMapStringDV).equals(expectedStringForMap) should be(true)
  }

  it should "Checking Vector Functionality" in {
    val expectedStringForVector = AccumData.withName("MULTILINEGRAPH")

    val arrayOfInt: Array[Int] = Array(1, 2, 3, 4)
    AccumData.getGraphType(data = arrayOfInt).equals(expectedStringForVector) should be(true)

    val arrayOfString: Array[String] = Array("1", "2", "3", "4")
    AccumData.getGraphType(data = arrayOfString).equals(expectedStringForVector) should be(true)

    val listOfInt: List[Int] = arrayOfInt.toList
    AccumData.getGraphType(data = listOfInt).equals(expectedStringForVector) should be(true)

    val setOfInt: Set[Int] = arrayOfInt.toList.toSet
    AccumData.getGraphType(data = setOfInt).equals(expectedStringForVector) should be(true)

    val seqOfInt: Seq[Int] = arrayOfInt.toSeq
    AccumData.getGraphType(data = seqOfInt).equals(expectedStringForVector) should be(true)

    val denseVectorInt: DenseVector[Int] = DenseVector(arrayOfInt)
    AccumData.getGraphType(data = denseVectorInt).equals(expectedStringForVector) should be(true)
  }

  it should "Checking Matrix Functionality" in {
    val expectedStringForMatrix = AccumData.withName("MATRIX")

    val matrixOfInt: DenseMatrix[Int] = DenseMatrix((1, 2, 3), (4, 5, 6))
    AccumData.getGraphType(data = matrixOfInt).equals(expectedStringForMatrix) should be(true)

    val matrixOfDouble: DenseMatrix[Double] = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))
    AccumData.getGraphType(data = matrixOfDouble).equals(expectedStringForMatrix) should be(true)
  }

  it should "Checking Unknown Functionality" in {
    val expectedStringForUnknown = AccumData.withName("UNKNOWN")

    val unknownType1: Char = 'a'
    AccumData.getGraphType(data = unknownType1).equals(expectedStringForUnknown) should be(true)

    val unknownType2: TestClassToTestUnknown = TestClassToTestUnknown(random = None)
    AccumData.getGraphType(data = unknownType2).equals(expectedStringForUnknown) should be(true)
  }
}

case class TestClassToTestUnknown(random: Option[String])
