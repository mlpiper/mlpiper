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

import org.apache.flink.streaming.scala.examples.common.parsing.ParameterIndices
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test
import org.scalatest.Matchers

class ParameterIndicesITCase extends StreamingMultipleProgramsTestBase with Matchers {

  private def testParameterIndices(indices: String, expectedOutput: Array[Int])
  : Boolean = {
    val paramIndices = new ParameterIndices(indices).toIterator
    var i: Int = 0
    var tmpIndex: Int = 0

    while (paramIndices.hasNext) {
      tmpIndex = paramIndices.next()
      if (tmpIndex != expectedOutput(i)) {
        return false
      }
      i += 1
    }

    i == expectedOutput.length
  }

  @Test def testSingleIndex(): Unit = {
    val input = "5"
    val expectedOutput = Array(5)

    testParameterIndices(input, expectedOutput) should be(true)
  }

  @Test def testMultipleIndices(): Unit = {
    val input = "5,4,6,9,10"
    val expectedOutput = Array(5,4,6,9,10)

    testParameterIndices(input, expectedOutput) should be(true)
  }

  @Test def testNonAscendingIndices(): Unit = {
    val input = "1,3,2"
    val expectedOutput = Array(1,3,2)

    testParameterIndices(input, expectedOutput) should be(true)
  }

  @Test def testDuplicateIndices(): Unit = {
    val input = "5,5,6,6,7,7,8,9,10"
    val expectedOutput = Array(5,5,6,6,7,7,8,9,10)

    testParameterIndices(input, expectedOutput) should be(true)
  }

  @Test def testSingleRange(): Unit = {
    val input = "1-5"
    val expectedOutput = Array(1,2,3,4,5)

    testParameterIndices(input, expectedOutput) should be(true)
  }

  @Test def testReverseRange(): Unit = {
    val input = "5-1"
    val expectedOutput = Array(5,4,3,2,1)

    testParameterIndices(input, expectedOutput) should be(true)
  }

  @Test def testMultipleRanges(): Unit = {
    val input = "1-5,8-9,12-10"
    val expectedOutput = Array(1,2,3,4,5,8,9,12,11,10)

    testParameterIndices(input, expectedOutput) should be(true)
  }

  @Test def testDuplicateRanges(): Unit = {
    val input = "2-4,2-4"
    val expectedOutput = Array(2,3,4,2,3,4)

    testParameterIndices(input, expectedOutput) should be(true)
  }

  @Test def testOverlappingRanges(): Unit = {
    val input = "2-5,3-7"
    val expectedOutput = Array(2,3,4,5,3,4,5,6,7)

    testParameterIndices(input, expectedOutput) should be(true)
  }

  @Test def testRangesAndIndices1(): Unit = {
    val input = "2-5,3,4,5,8"
    val expectedOutput = Array(2,3,4,5,3,4,5,8)

    testParameterIndices(input, expectedOutput) should be(true)
  }

  @Test def testRangesAndIndices2(): Unit = {
    val input = "1,5,2-6"
    val expectedOutput = Array(1,5,2,3,4,5,6)

    testParameterIndices(input, expectedOutput) should be(true)
  }

  @Test def testRangesAndIndicesSize(): Unit = {
    val input = "2-5,3,4,5,8"
    val paramIndices = new ParameterIndices(input)

    paramIndices.size should be(8)
  }

  @Test def testNullInput(): Unit = {
    val indices = null
    an [IllegalArgumentException] should be thrownBy new ParameterIndices(indices)
  }

  @Test def testEmptyInput(): Unit = {
    val indices = ""
    an [IllegalArgumentException] should be thrownBy new ParameterIndices(indices)
  }

  @Test def testNegativeIndex(): Unit = {
    val indices = "-1"
    an [IllegalArgumentException] should be thrownBy new ParameterIndices(indices)
  }

  @Test def testNegativeRange(): Unit = {
    val indices = "-4-5"
    an [IllegalArgumentException] should be thrownBy new ParameterIndices(indices)
  }

  @Test def testUnfinishedRange1(): Unit = {
    val indices = "5-3,6-"
    an [IllegalArgumentException] should be thrownBy new ParameterIndices(indices)
  }

  @Test def testUnfinishedRange2(): Unit = {
    val indices = "2-"
    an [IllegalArgumentException] should be thrownBy new ParameterIndices(indices)
  }

  @Test def testUnfinishedRange3(): Unit = {
    val indices = "1,2-,4"
    an [IllegalArgumentException] should be thrownBy new ParameterIndices(indices)
  }

  @Test def testNoElements(): Unit = {
    val indices = ","
    an [IllegalArgumentException] should be thrownBy new ParameterIndices(indices)
  }

  @Test def testGarbageElements(): Unit = {
    val indices = "sadf,2342,sfd,asf3"
    an [IllegalArgumentException] should be thrownBy new ParameterIndices(indices)
  }
}