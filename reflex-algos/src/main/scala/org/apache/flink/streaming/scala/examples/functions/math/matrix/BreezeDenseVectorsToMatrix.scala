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
package org.apache.flink.streaming.scala.examples.functions.math.matrix

import breeze.linalg.{DenseMatrix => BreezeDenseMatrix, DenseVector => BreezeDenseVector}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.scala.examples.functions.map.PrependSubtaskID
import org.apache.flink.streaming.scala.examples.functions.window.SubtaskWindowBatch

object BreezeDenseVectorsToMatrix {

  /**
    * Converts Iterator of BreezeDenseVector to a BreezeDenseMatrix
    */
  def iteratorToMatrix(iterator: Iterator[BreezeDenseVector[Double]])
  : BreezeDenseMatrix[Double] = {
    val (a, b) = iterator.duplicate
    val (c, d) = a.duplicate
    val length = b.length
    val attributes = c.next().length

    val matrix = new BreezeDenseMatrix[Double](length, attributes)
    for (i <- 0 until length) {
      matrix(i, ::) := d.next().t
    }
    matrix
  }

  /**
    * Converts Iterable of BreezeDenseVector to a BreezeDenseMatrix
    */
  def iterableToMatrix(iterable: Iterable[BreezeDenseVector[Double]])
  : BreezeDenseMatrix[Double] = {
    iteratorToMatrix(iterable.toIterator)
  }

  //////////////////////////////////////////////////////////////////////////////////////
  // DataStream
  //////////////////////////////////////////////////////////////////////////////////////

  /**
    * Converts DataStream of Iterable of BreezeDenseVector to BreezeDenseMatrix
    */
  def dataStreamToMatrices(inputStream: DataStream[Iterable[BreezeDenseVector[Double]]])
  : DataStream[BreezeDenseMatrix[Double]] = {
    inputStream.map(iterableToMatrix _)
  }

  /**
    * Converts DataStream of BreezeDenseVector to BreezeDenseMatrix
    * using a local count window per matrix.
    */
  def dataStreamToMatrices(inputStream: DataStream[BreezeDenseVector[Double]],
                           size: Long)
  : DataStream[BreezeDenseMatrix[Double]] = {
    dataStreamToMatrices(SubtaskWindowBatch.countBatch(inputStream, size))
  }

  /**
    * Converts DataStream of BreezeDenseVector to BreezeDenseMatrix
    * using a local time window per matrix.
    */
  def dataStreamToMatrices(inputStream: DataStream[BreezeDenseVector[Double]],
                           size: Time)
  : DataStream[BreezeDenseMatrix[Double]] = {
    dataStreamToMatrices(SubtaskWindowBatch.timeBatch(inputStream, size))
  }

  //////////////////////////////////////////////////////////////////////////////////////
  // DataSet
  //////////////////////////////////////////////////////////////////////////////////////

  /**
    * Converts DataSet of BreezeDenseVector to BreezeDenseMatrix using local data per matrix.
    */
  def dataSetToMatrices(inputStream: DataSet[BreezeDenseVector[Double]])
  : DataSet[BreezeDenseMatrix[Double]] = {
    inputStream
      .map(new PrependSubtaskID[BreezeDenseVector[Double]])
      .groupBy(_.subtaskID)
      .reduceGroup(x => {
        lazy val vectors = x.map(_.element)
        iteratorToMatrix(vectors)
      })
  }
}
