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

import breeze.linalg.{DenseVector => BreezeDenseVector}
import org.apache.flink.streaming.scala.examples.clustering.math.LabeledVector
import org.apache.flink.streaming.scala.examples.common.parsing.ParameterIndices

object StringToIndexedVectorTestData {
  val validVectorNrElements: Int = 11
  val validVectorStringsCommaSeparated: Seq[String] = Seq[String](
    "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10",
    "0,  10,   20,  30,40,   50, 60 ,  70 ,   80,90,100",
    "0,100.234,200,300,400, 500,600,700, 800,900,1.2",
    "0,-1,-2,-3,-4,  -5,  -6, -7, -8,-9,-10",
    "0,-10,-20,-30,-40,-50,-60,-70,-80,-90,-100",
    "0, -1.34 ,-20.0  ,-30,-400,-500,-600,-700,-800,-9,-1")

  val validVectorStringsSpaceSeparated: Seq[String] = Seq[String](
    "0 1 2 3 4 5 6 7 8 9 10",
    "0  10   20  30 40   50 60   70    80 90 100",
    "0 100.234 200 300 400  500 600 700  800 900 1.2",
    "0 -1 -2 -3 -4   -5   -6  -7  -8 -9 -10",
    "0 -10 -20 -30 -40 -50 -60 -70 -80 -90 -100",
    "0  -1.34  -20.0  -30 -400 -500 -600 -700 -800 -9 -1")

  val validVectors: Seq[BreezeDenseVector[Double]] = Seq[BreezeDenseVector[Double]](
    new BreezeDenseVector[Double](Array(0.0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)),
    new BreezeDenseVector[Double](Array(0.0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100)),
    new BreezeDenseVector[Double](Array(0.0, 100.234, 200, 300, 400, 500, 600, 700, 800, 900, 1.2)),
    new BreezeDenseVector[Double](Array(0.0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10)),
    new BreezeDenseVector[Double](Array(0.0, -10, -20, -30, -40, -50, -60, -70, -80, -90, -100)),
    new BreezeDenseVector[Double](Array(0.0, -1.34, -20, -30, -400, -500, -600, -700, -800, -9, -1))
  )

  val invalidVectorNrElements: Int = 5
  val invalidVectorsCommaSeparated: Seq[String] = Seq[String](
    "0,1,2,3,4,5",
    "0,1,2,,3,4",
    "0,1,2,3",
    "0, 1, 2, 3, 4, ,",
    ",0,1,2,3,4")

  /**
    * Iterates over parsedOutput assuring it matches the elements of expectedOutput with respect
    * to indices.
    *
    * @param parsedOutput   Parsed output to compare against expectedOutput.
    * @param expectedOutput Contains all elements parsedOutput could contain.
    * @param indices        Indices j of expectedOutput, where each indices literal index i is the index in
    *                       parsedOutput that should equal the jth element of expectedOutput.
    * @return True if parsedOutput was parsed correctly. False otherwise.
    */
  private def compareBreezeOutputVector(parsedOutput: BreezeDenseVector[Double],
                                        expectedOutput: BreezeDenseVector[Double],
                                        indices: ParameterIndices)
  : Boolean = {
    if (parsedOutput.length != indices.size) {
      return false
    }

    val indexIterator = indices.iterator
    var parsedOutputIdx: Int = 0
    var matchingIndex: Int = 0
    while (indexIterator.hasNext) {
      matchingIndex = indexIterator.next()
      if (parsedOutput(parsedOutputIdx) != expectedOutput(matchingIndex)) {
        return false
      }
      parsedOutputIdx += 1
    }

    parsedOutputIdx == parsedOutput.length
  }

  /**
    * Compares parsedOutput vectors with expectedOutput vectors.
    *
    * @param parsedOutput   Output from the parsing coflatmap function.
    * @param expectedOutput Expected output that contains all elements in parsedOutput.
    * @param indices        Indices to extract data from expectedOutput.
    * @return True all vectors in parsedOutput were parsed correctly. False otherwise.
    */
  def compareBreezeOutputStream(parsedOutput: Seq[BreezeDenseVector[Double]],
                                expectedOutput: Seq[BreezeDenseVector[Double]],
                                indices: ParameterIndices)
  : Boolean = {
    if (parsedOutput.length != expectedOutput.length) {
      return false
    }

    for (i <- parsedOutput.indices) {
      if (!compareBreezeOutputVector(parsedOutput(i), expectedOutput(i), indices)) {
        return false
      }
    }
    true
  }

  /**
    * Compares parsedOutput vectors with expectedOutput vectors.
    *
    * @param parsedOutput   Output from the parsing coflatmap function.
    * @param expectedOutput Expected output that contains all elements in parsedOutput.
    * @param indices        Indices to extract data from expectedOutput.
    * @return True all vectors in parsedOutput were parsed correctly. False otherwise.
    */
  def compareLabeledVectorOutputStream(parsedOutput: Seq[LabeledVector[Double]],
                                       expectedOutput: Seq[BreezeDenseVector[Double]],
                                       indices: ParameterIndices,
                                       labelIndex: Option[Int],
                                       timestampIndex: Option[Int])
  : Boolean = {
    if (parsedOutput.length != expectedOutput.length) {
      return false
    }

    // Compare vector portions
    for (i <- parsedOutput.indices) {
      if (!compareBreezeOutputVector(parsedOutput(i).vector, expectedOutput(i), indices)) {
        return false
      }
    }

    // Compare label if defined
    if (labelIndex.isDefined) {
      val labelIdx = labelIndex.get
      for (i <- parsedOutput.indices) {
        if (parsedOutput(i).label != expectedOutput(i)(labelIdx)) {
          return false
        }
      }
    }

    // Compare timestamp if defined
    if (timestampIndex.isDefined) {
      val timestampIdx = timestampIndex.get
      for (i <- parsedOutput.indices) {
        if (parsedOutput(i).timestamp != expectedOutput(i)(timestampIdx).toLong) {
          return false
        }
      }
    }

    true
  }

  // TODO: extend to receive compare func
  def compareStringOutputStream[T](parsedOutput: Iterable[T],
                                   expectedOutput: Iterable[T])
  : Boolean = {

    if (parsedOutput.size != expectedOutput.size) {
      return false
    }
    parsedOutput.zip(expectedOutput).filter(x => x._1 != x._2).isEmpty
  }
}


