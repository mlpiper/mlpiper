/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.test.exampleScalaPrograms.clustering

import java.io.{OutputStream, PrintStream}

import org.apache.flink.contrib.streaming.scala.utils.DataStreamUtils
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.scala.examples.flink.utils.functions.conversion.StringToIndexedLabeledVectorFlatMap
import org.apache.flink.streaming.test.exampleScalaPrograms.clustering.StringToIndexedVectorTestData._
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test
import org.mlpiper.utils.ParameterIndices
import org.scalatest.Matchers

class StringToIndexedLabeledVectorITCase extends StreamingMultipleProgramsTestBase with Matchers {

  ////////////////////////////////////////////////////////////////////////
  // COMMA-SEPARATED TESTS
  ////////////////////////////////////////////////////////////////////////

  @Test def testAllIndicesCommaSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = None
    val timestampIndex: Option[Int] = None

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsCommaSeparated)
    val indices = new ParameterIndices(validVectorNrElements)

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, ",", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testSingleIndexCommaSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsCommaSeparated)
    val indices = new ParameterIndices("4")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, ",", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testMultipleIndicesCommaSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = None
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsCommaSeparated)
    val indices = new ParameterIndices("5,4,6,9,10")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, ",", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testDuplicateIndicesCommaSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = None

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsCommaSeparated)
    val indices = new ParameterIndices("5,5,6,6,7,7,8,9,10")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, ",", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testSingleRangeCommaSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsCommaSeparated)
    val indices = new ParameterIndices("1-5")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, ",", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testReverseRangeCommaSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsCommaSeparated)
    val indices = new ParameterIndices("5-1")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, ",", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testMultipleRangesCommaSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsCommaSeparated)
    val indices = new ParameterIndices("1-5,8-9,10-6")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, ",", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testDuplicateRangesCommaSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(validVectorNrElements - 1)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsCommaSeparated)
    val indices = new ParameterIndices("2-4,2-4")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, ",", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testOverlappingRangesCommaSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsCommaSeparated)
    val indices = new ParameterIndices("2-5,3-7")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, ",", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testRangesAndIndicesCommaSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(0)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsCommaSeparated)
    val indices = new ParameterIndices("2-5,3,4,5,8")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, ",", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testRangesAndIndicesSizeCommaSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(0)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsCommaSeparated)
    val indices = new ParameterIndices("2-5,3,4,5,8")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, ",", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  ////////////////////////////////////////////////////////////////////////
  // NEGATIVE TEST
  ////////////////////////////////////////////////////////////////////////

  @Test def testInvalidVectorsCommaSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(6)

    // Want to suppress println's, so we will temporarily point System.out to the equivalent of
    // /dev/null
    val out = System.out
    System.setOut(new PrintStream(new OutputStream() {
      override def write(b: Int): Unit = { /* Do nothing */ }
    }))

    val textStream: DataStream[String] = env.fromCollection(invalidVectorsCommaSeparated)
    val indices = new ParameterIndices(invalidVectorNrElements)

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, ",", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    // Restore System.out
    System.setOut(out)
    output.size should be(0)
  }

  @Test def testNegativeLabelIndex(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val nrElements = 10

    val labelIndex: Option[Int] = Some(-1)
    val timestampIndex: Option[Int] = Some(6)

    val indices = new ParameterIndices(nrElements)

    an [IllegalArgumentException] should be thrownBy new StringToIndexedLabeledVectorFlatMap(
      nrElements, ",", indices, labelIndex, timestampIndex)
  }

  @Test def testOutOfBoundsLabelIndex(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val nrElements = 10

    val labelIndex: Option[Int] = Some(10)
    val timestampIndex: Option[Int] = Some(6)

    val indices = new ParameterIndices(nrElements)

    an [IllegalArgumentException] should be thrownBy new StringToIndexedLabeledVectorFlatMap(
      nrElements, ",", indices, labelIndex, timestampIndex)
  }

  @Test def testNegativeTimestampIndex(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val nrElements = 10

    val labelIndex: Option[Int] = Some(6)
    val timestampIndex: Option[Int] = Some(-1)

    val indices = new ParameterIndices(nrElements)

    an [IllegalArgumentException] should be thrownBy new StringToIndexedLabeledVectorFlatMap(
      nrElements, ",", indices, labelIndex, timestampIndex)
  }

  @Test def testOutOfBoundsTimestampIndex(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val nrElements = 10

    val labelIndex: Option[Int] = Some(6)
    val timestampIndex: Option[Int] = Some(10)

    val indices = new ParameterIndices(nrElements)

    an [IllegalArgumentException] should be thrownBy new StringToIndexedLabeledVectorFlatMap(
      nrElements, ",", indices, labelIndex, timestampIndex)
  }

  ////////////////////////////////////////////////////////////////////////
  // SPACE-SEPARATED TESTS
  ////////////////////////////////////////////////////////////////////////

  @Test def testAllIndicesSpaceSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsSpaceSeparated)
    val indices = new ParameterIndices(validVectorNrElements)

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, "\\s+", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testSingleIndexSpaceSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsSpaceSeparated)
    val indices = new ParameterIndices("4")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, "\\s+", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testMultipleIndicesSpaceSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = None

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsSpaceSeparated)
    val indices = new ParameterIndices("5,4,6,9,10")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, "\\s+", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testDuplicateIndicesSpaceSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsSpaceSeparated)
    val indices = new ParameterIndices("5,5,6,6,7,7,8,9,10")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, "\\s+", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testSingleRangeSpaceSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = None
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsSpaceSeparated)
    val indices = new ParameterIndices("1-5")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, "\\s+", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testReverseRangeSpaceSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsSpaceSeparated)
    val indices = new ParameterIndices("5-1")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, "\\s+", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testMultipleRangesSpaceSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsSpaceSeparated)
    val indices = new ParameterIndices("1-5,8-9,10-6")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, "\\s+", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testDuplicateRangesSpaceSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = None

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsSpaceSeparated)
    val indices = new ParameterIndices("2-4,2-4")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, "\\s+", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testOverlappingRangesSpaceSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsSpaceSeparated)
    val indices = new ParameterIndices("2-5,3-7")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, "\\s+", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testRangesAndIndicesSpaceSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = Some(3)
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsSpaceSeparated)
    val indices = new ParameterIndices("2-5,3,4,5,8")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, "\\s+", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

  @Test def testRangesAndIndicesSizeSpaceSeparated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val labelIndex: Option[Int] = None
    val timestampIndex: Option[Int] = Some(6)

    val textStream: DataStream[String] = env.fromCollection(validVectorStringsSpaceSeparated)
    val indices = new ParameterIndices("2-5,3,4,5,8")

    val flatMap = new StringToIndexedLabeledVectorFlatMap(
      validVectorNrElements, "\\s+", indices, labelIndex, timestampIndex)

    val output = DataStreamUtils(textStream.flatMap(flatMap)).collect().toSeq

    compareLabeledVectorOutputStream(
      output, validVectors, indices, labelIndex, timestampIndex) should be(true)
  }

}
