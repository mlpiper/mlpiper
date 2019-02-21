package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import java.io.File

import breeze.linalg.{DenseMatrix, DenseVector => BreezeDenseVector}
import org.apache.flink.streaming.scala.examples.clustering.math.{LabeledVector, ReflexColumnEntry, ReflexNamedVector, ReflexPrediction}
import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils
import org.apache.flink.streaming.scala.examples.common.algorithm.PredictionOutput
import org.apache.flink.streaming.scala.examples.common.ml.InferenceConstants
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import com.parallelmachines.reflex.common.enums.OpType
import org.junit.Test
import org.scalatest.Matchers

import scala.collection.mutable

class ParsingUtilsTest extends StreamingMultipleProgramsTestBase with Matchers {

  /** Test parsing a vector where the elements are separated by the elementSeparator. */
  @Test def testStringToVector1(): Unit = {
    val vectorString = "1,2,3,4,5.0324,6"
    val vectorExpectedOutput = new BreezeDenseVector[Double](Array[Double](1, 2, 3, 4, 5.0324, 6))

    val parsedVector = ParsingUtils.stringToBreezeDenseVector(vectorString, ',')

    parsedVector.isDefined should be(true)
    parsedVector.get.equals(vectorExpectedOutput) should be(true)
  }

  /** Test parsing a vector where the elements are separated by the elementSeparator and space. */
  @Test def testStringToVector2(): Unit = {
    val vectorString = "1, 2, 3, 4, 5.0324, 6"
    val vectorExpectedOutput = new BreezeDenseVector[Double](Array[Double](1, 2, 3, 4, 5.0324, 6))

    val parsedVector = ParsingUtils.stringToBreezeDenseVector(vectorString, ',')

    parsedVector.isDefined should be(true)
    parsedVector.get.equals(vectorExpectedOutput) should be(true)
  }

  /** Test parsing a vector where one element is not separated by the elementSeparator */
  @Test def testStringToVector3(): Unit = {
    val vectorString = "1, 2, 3, 4 5.0324, 6"
    val parsedVector = ParsingUtils.stringToBreezeDenseVector(vectorString, ',')
    parsedVector.isDefined should be(false)
  }

  /** Test parsing a vector with a single element */
  @Test def testStringToVector4(): Unit = {
    val vectorString = "253.360"
    val vectorExpectedOutput = new BreezeDenseVector[Double](Array[Double](253.360))

    val parsedVector = ParsingUtils.stringToBreezeDenseVector(vectorString, ',')

    parsedVector.isDefined should be(true)
    parsedVector.get.equals(vectorExpectedOutput) should be(true)
  }

  /** Test parsing a vector with various ways to represent a double */
  @Test def testStringToVector5(): Unit = {
    val vectorString = "-1.1,-23.34,3e10,-4e-3,-2.1e-4,0123,.5, -.1"
    val vectorExpectedOutput
    = new BreezeDenseVector[Double](
      Array[Double](-1.1, -23.34, 3e10, -4e-3, -2.1e-4, 123, 0.5, -0.1))

    val parsedVector = ParsingUtils.stringToBreezeDenseVector(vectorString, ',')

    parsedVector.isDefined should be(true)
    parsedVector.get.equals(vectorExpectedOutput) should be(true)
  }

  /**
    * Test parsing a LabeledVector where the elements are separated by the elementSeparator, with
    * no label.
    */
  @Test def testStringToLabeledVector1(): Unit = {
    val vectorString = "1,2,3,4,5.0324,6"
    val vectorExpectedOutput = new LabeledVector[Double](Array[Double](1, 2, 3, 4, 5.0324, 6))

    val parsedVector = ParsingUtils.stringToLabeledVector(vectorString, ',')

    parsedVector.isDefined should be(true)
    parsedVector.get.equals(vectorExpectedOutput) should be(true)
  }

  /**
    * Test parsing a LabeledVector where the elements are separated by the elementSeparator and
    * space, with no label.
    */
  @Test def testStringToLabeledVector2(): Unit = {
    val vectorString = "1, 2, 3, 4, 5.0324, 6"
    val vectorExpectedOutput = new LabeledVector[Double](Array[Double](1, 2, 3, 4, 5.0324, 6))

    val parsedVector = ParsingUtils.stringToLabeledVector(vectorString, ',')

    parsedVector.isDefined should be(true)
    parsedVector.get.equals(vectorExpectedOutput) should be(true)
  }

  /**
    * Test parsing a LabeledVector where one element is not separated by the elementSeparator, no
    * label.
    */
  @Test def testStringToLabeledVector3(): Unit = {
    val vectorString = "1, 2, 3, 4 5.0324, 6"
    val parsedVector = ParsingUtils.stringToLabeledVector(vectorString, ',')
    parsedVector.isDefined should be(false)
  }

  /** Test parsing a LabeledVector with a single element, no label. */
  @Test def testStringToLabeledVector4(): Unit = {
    val vectorString = "253.360"
    val vectorExpectedOutput = new LabeledVector[Double](Array[Double](253.360))

    val parsedVector = ParsingUtils.stringToLabeledVector(vectorString, ',')

    parsedVector.isDefined should be(true)
    parsedVector.get.equals(vectorExpectedOutput) should be(true)
  }

  /**
    * Test parsing a LabeledVector where the elements are separated by the elementSeparator, with
    * a label.
    */
  @Test def testStringToLabeledVector5(): Unit = {
    val vectorString = "-98324.342; 1,2,3,4,5.0324,6"
    val vectorExpectedOutput
    = new LabeledVector[Double](-98324.342, Array[Double](1, 2, 3, 4, 5.0324, 6))

    val parsedVector = ParsingUtils.stringToLabeledVector(vectorString, ',', Some(';'))

    parsedVector.isDefined should be(true)
    parsedVector.get.equals(vectorExpectedOutput) should be(true)
  }

  /**
    * Test parsing a LabeledVector where the elements are separated by the elementSeparator and
    * space, with a label.
    */
  @Test def testStringToLabeledVector6(): Unit = {
    val vectorString = "234.3; 1, 2, 3, 4, 5.0324, 6"
    val vectorExpectedOutput
    = new LabeledVector[Double](234.3, Array[Double](1, 2, 3, 4, 5.0324, 6))

    val parsedVector = ParsingUtils.stringToLabeledVector(vectorString, ',', Some(';'))

    parsedVector.isDefined should be(true)
    parsedVector.get.equals(vectorExpectedOutput) should be(true)
  }

  /**
    * Test parsing a LabeledVector where one element is not separated by the elementSeparator, with
    * a label.
    */
  @Test def testStringToLabeledVector7(): Unit = {
    val vectorString = "-82.2; 1, 2, 3, 4 5.0324, 6"
    val parsedVector = ParsingUtils.stringToLabeledVector(vectorString, ',', Some(';'))
    parsedVector.isDefined should be(false)
  }

  /** Test parsing a LabeledVector with a single element, with a label. */
  @Test def testStringToLabeledVector8(): Unit = {
    val vectorString = "-1.0; 253.360"
    val vectorExpectedOutput = new LabeledVector[Double](-1.0, Array[Double](253.360))

    val parsedVector = ParsingUtils.stringToLabeledVector(vectorString, ',', Some(';'))

    parsedVector.isDefined should be(true)
    parsedVector.get.equals(vectorExpectedOutput) should be(true)
  }

  /** Test parsing a LabeledVector where there are multiple label separators. */
  @Test def testStringToLabeledVector9(): Unit = {
    val vectorString = "-82.2; 1; 2, 3, 4, 5.0324, 6"
    val parsedVector = ParsingUtils.stringToLabeledVector(vectorString, ',', Some(';'))
    parsedVector.isDefined should be(false)
  }

  /**
    * Test parsing a LabeledVector,Timestamp where the elements are separated by the
    * elementSeparator, with no label.
    */
  @Test def testStringToLabeledVectorTimestamp1(): Unit = {
    val vectorString = "1,2,3,4,5.0324,6`2342"
    val vectorExpectedOutput
    = new LabeledVector[Double](Array[Double](1, 2, 3, 4, 5.0324, 6), 2342L)

    val timestampExpectedOutput = 2342L

    val parsedVectorTimestamp = ParsingUtils.stringToLabeledVector(
      labeledVectorInput = vectorString,
      elementSeparator = ',',
      labelSeparator = None,
      timestampSeparator = Some('`'))

    parsedVectorTimestamp.isDefined should be(true)

    val outputVectorTimestamp = parsedVectorTimestamp.get

    outputVectorTimestamp.equals(vectorExpectedOutput) should be(true)
    outputVectorTimestamp.timestamp.equals(timestampExpectedOutput) should be(true)
  }

  /**
    * Test parsing a LabeledVector,Timestamp where the elements are separated by the
    * elementSeparator and space, with no label.
    */
  @Test def testStringToLabeledVectorTimestamp2(): Unit = {
    val vectorString = "1, 2, 3, 4, 5.0324, 6 ` 3423"
    val vectorExpectedOutput = new LabeledVector[Double](Array[Double](1, 2, 3, 4, 5.0324, 6))
    val timestampExpectedOutput = 3423L

    val parsedVectorTimestamp = ParsingUtils.stringToLabeledVector(
      labeledVectorInput = vectorString,
      elementSeparator = ',',
      labelSeparator = None,
      timestampSeparator = Some('`'))
    parsedVectorTimestamp.isDefined should be(true)

    val outputVectorTimestamp = parsedVectorTimestamp.get

    outputVectorTimestamp.equals(vectorExpectedOutput) should be(true)
    outputVectorTimestamp.timestamp.equals(timestampExpectedOutput) should be(true)
  }

  /**
    * Test parsing a LabeledVector,Timestamp where one element is not separated by the
    * elementSeparator, no label.
    */
  @Test def testStringToLabeledVectorTimestamp3(): Unit = {
    val vectorString = "1, 2, 3, 4 5.0324, 6`4345"
    val parsedVector = ParsingUtils.stringToLabeledVector(
      labeledVectorInput = vectorString,
      elementSeparator = ',',
      labelSeparator = None,
      timestampSeparator = Some('`'))

    parsedVector.isDefined should be(false)
  }

  /** Test parsing a LabeledVector,Timestamp with a single element, no label. */
  @Test def testStringToLabeledVectorTimestamp4(): Unit = {
    val vectorString = "253.360`123"
    val vectorExpectedOutput = new LabeledVector[Double](Array[Double](253.360))
    val timestampExpectedOutput = 123L

    val parsedVectorTimestamp = ParsingUtils.stringToLabeledVector(
      labeledVectorInput = vectorString,
      elementSeparator = ',',
      labelSeparator = None,
      timestampSeparator = Some('`'))

    parsedVectorTimestamp.isDefined should be(true)

    val vectorTimestamp = parsedVectorTimestamp.get

    vectorTimestamp.equals(vectorExpectedOutput) should be(true)
    vectorTimestamp.timestamp.equals(timestampExpectedOutput) should be(true)
  }

  /**
    * Test parsing a LabeledVector,Timestamp where the elements are separated by the
    * elementSeparator, with a label.
    */
  @Test def testStringToLabeledVectorTimestamp5(): Unit = {
    val vectorString = "-98324.342; 1,2,3,4,5.0324,6`769"
    val vectorExpectedOutput
    = new LabeledVector[Double](-98324.342, Array[Double](1, 2, 3, 4, 5.0324, 6))
    val timestampExpectedOutput = 769L

    val parsedVectorTimestamp = ParsingUtils.stringToLabeledVector(
      labeledVectorInput = vectorString,
      elementSeparator = ',',
      labelSeparator = Some(';'),
      timestampSeparator = Some('`'))

    parsedVectorTimestamp.isDefined should be(true)

    val vectorTimestamp = parsedVectorTimestamp.get

    vectorTimestamp.equals(vectorExpectedOutput) should be(true)
    vectorTimestamp.timestamp.equals(timestampExpectedOutput) should be(true)
  }

  /**
    * Test parsing a LabeledVector,Timestamp where the elements are separated by the
    * elementSeparator and space, with a label.
    */
  @Test def testStringToLabeledVectorTimestamp6(): Unit = {
    val vectorString = "234.3; 1, 2, 3, 4, 5.0324, 6 ` 232"
    val vectorExpectedOutput
    = new LabeledVector[Double](234.3, Array[Double](1, 2, 3, 4, 5.0324, 6))
    val timestampExpectedOutput = 232L

    val parsedVectorTimestamp = ParsingUtils.stringToLabeledVector(
      labeledVectorInput = vectorString,
      elementSeparator = ',',
      labelSeparator = Some(';'),
      timestampSeparator = Some('`'))
    parsedVectorTimestamp.isDefined should be(true)

    val vectorTimestamp = parsedVectorTimestamp.get

    vectorTimestamp.equals(vectorExpectedOutput) should be(true)
    vectorTimestamp.timestamp.equals(timestampExpectedOutput) should be(true)
  }

  /**
    * Test parsing a LabeledVector,Timestamp where one element is not separated by the
    * elementSeparator, with a label.
    */
  @Test def testStringToLabeledVectorTimestamp7(): Unit = {
    val vectorString = "-82.2; 1, 2, 3, 4 5.0324, 6`123"
    val parsedVector = ParsingUtils.stringToLabeledVector(
      labeledVectorInput = vectorString,
      elementSeparator = ',',
      labelSeparator = None,
      timestampSeparator = Some('`'))

    parsedVector.isDefined should be(false)
  }

  /** Test parsing a LabeledVector,Timestamp with a single element, with a label. */
  @Test def testStringToLabeledVectorTimestamp8(): Unit = {
    val vectorString = "-1.0; 253.360` 123"
    val vectorExpectedOutput = new LabeledVector[Double](-1.0, Array[Double](253.360))
    val timestampExpectedOutput = 123L

    val parsedVectorTimestamp = ParsingUtils.stringToLabeledVector(
      labeledVectorInput = vectorString,
      elementSeparator = ',',
      labelSeparator = Some(';'),
      timestampSeparator = Some('`'))
    parsedVectorTimestamp.isDefined should be(true)

    val vectorTimestamp = parsedVectorTimestamp.get

    vectorTimestamp.equals(vectorExpectedOutput) should be(true)
    vectorTimestamp.timestamp.equals(timestampExpectedOutput) should be(true)
  }

  /**
    * Test parsing a LabeledVector,Timestamp where there is an extra elementSeparator prepended to
    * the vector.
    */
  @Test def testStringToLabeledVectorTimestamp9(): Unit = {
    val vectorString = "-0.1;,1,2`12"
    val parsedVector = ParsingUtils.stringToLabeledVector(
      labeledVectorInput = vectorString,
      elementSeparator = ',',
      labelSeparator = None,
      timestampSeparator = Some('`'))

    parsedVector.isDefined should be(false)
  }

  /** Test parsing a LabeledVector,Timestamp where there are two labels. */
  @Test def testStringToLabeledVectorTimestamp10(): Unit = {
    val vectorString = "-1,1;1,1,1`1"
    val parsedVector = ParsingUtils.stringToLabeledVector(
      labeledVectorInput = vectorString,
      elementSeparator = ',',
      labelSeparator = None,
      timestampSeparator = Some('`'))

    parsedVector.isDefined should be(false)
  }

  /**
    * Test parsing a LabeledVector,Timestamp where there is an extra elementSeparator appended to
    * the vector.
    */
  @Test def testStringToLabeledVectorTimestamp11(): Unit = {
    val vectorString = "-1;1,1,1,`1"
    val parsedVector = ParsingUtils.stringToLabeledVector(
      labeledVectorInput = vectorString,
      elementSeparator = ',',
      labelSeparator = None,
      timestampSeparator = Some('`'))

    parsedVector.isDefined should be(false)
  }

  /** Test parsing a LabeledVector,Timestamp where there are two timestamps. */
  @Test def testStringToLabeledVectorTimestamp13(): Unit = {
    val vectorString = "-1;1,1,1`1,1"
    val parsedVector = ParsingUtils.stringToLabeledVector(
      labeledVectorInput = vectorString,
      elementSeparator = ',',
      labelSeparator = None,
      timestampSeparator = Some('`'))

    parsedVector.isDefined should be(false)
  }

  @Test def testMapToStringTest(): Unit = {
    val map = Map[String, String]("1" -> "1.0,2.0,3.0,4.0,5.0324,6.0", "2" -> "1.0,2.0")
    val expectedStringRep = "{1:1.0,2.0,3.0,4.0,5.0324,6.0},{2:1.0,2.0}"

    ParsingUtils.mapToString(map = map, keyValueSeparator = ':', rowSeparator = ',') should be(expectedStringRep)
  }


  @Test def testMatrixToString(): Unit = {
    val matrix = DenseMatrix(BreezeDenseVector(1.0, 2.0), BreezeDenseVector(3.0, 4.0), BreezeDenseVector(-5.0, 6.0))
    val expectedJSON = "{2:-5.0,6.0},{1:3.0,4.0},{0:1.0,2.0}"

    ParsingUtils.breezeDenseMatrixToString(matrix, elementSeparator = ',').get should be(expectedJSON)
  }

  @Test def testStringToLocalStoreInval(): Unit = {
    val encodedModelString = new String("0001x")
    val localDir = ParsingUtils.stringToLocalStore(encodedModelString, "/tmp")

    localDir.isDefined should be(false)
  }

  @Test def testVectorToJson(): Unit = {
    val vector = new BreezeDenseVector[Double](Array[Double](1, 2, 3, 4, 5.0324, 6))
    val expectedJSON = "[1.0,2.0,3.0,4.0,5.0324,6.0]"

    ParsingUtils.breezeDenseVectorToJSON(vector) should be(expectedJSON)
  }

  @Test def testMatrixToJson(): Unit = {
    val matrix = DenseMatrix(BreezeDenseVector(1.0, 2.0), BreezeDenseVector(3.0, 4.0), BreezeDenseVector(-5.0, 6.0))
    val expectedJSON = "{\"2\":\"[-5.0,6.0]\",\"1\":\"[3.0,4.0]\",\"0\":\"[1.0,2.0]\"}"

    ParsingUtils.breezeDenseMatrixToJSON(matrix) should be(expectedJSON)
  }

  @Test def testDenseMatrixToString(): Unit = {
    val matrix = DenseMatrix(BreezeDenseVector(-5.0, 6.0), BreezeDenseVector(3.0, 4.0), BreezeDenseVector(1.0, 2.0))
    val expectedString = "-5.0,6.0\n3.0,4.0\n1.0,2.0"
    val columnSeparator = ','
    val rowSeparator = '\n'
    val resultString = ParsingUtils.denseMatrixToString(matrix, columnSeparator, rowSeparator)

    (resultString == expectedString) should be(true)
  }

  @Test def testDenseMatrixFromString(): Unit = {
    val expectedMatrix = DenseMatrix(BreezeDenseVector(-5.0, 6.0), BreezeDenseVector(3.0, 4.0), BreezeDenseVector(1.0, 2.0))
    val string = "-5.0,6.0\n3.0,4.0\n1.0,2.0"
    val columnSeparator = ','
    val rowSeparator = '\n'
    val resultMatrix = ParsingUtils.denseMatrixFromString(string, columnSeparator, rowSeparator)

    resultMatrix.isDefined should be(true)
    (expectedMatrix == resultMatrix.getOrElse(None)) should be(true)
  }

  @Test def testMapToJson(): Unit = {
    val map = mutable.Map[Int, List[Double]](
      1 -> List(10.0, 6.0, 4.0),
      2 -> List(15.0, 93.0)
    )

    // There is no ordering in the map, so we must check both cases
    val expectedMapJSON1 = "{\"1\":[10.0,6.0,4.0],\"2\":[15.0,93.0]}"
    val expectedMapJSON2 = "{\"2\":[15.0,93.0],\"1\":[10.0,6.0,4.0]}"

    val mapJSON = ParsingUtils.iterableToJSON(map)
    val equals = mapJSON == expectedMapJSON1 || mapJSON == expectedMapJSON2

    equals should be(true)
  }
}