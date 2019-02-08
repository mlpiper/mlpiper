package org.mlpiper.utils

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.scala.examples.flink.utils.functions.map.PrependSubtaskID
import org.apache.flink.streaming.scala.examples.flink.utils.functions.window.SubtaskWindowBatch

object BreezeDenseVectorsToMatrix {

  /**
    * Converts Iterator of DenseVector to a BreezeDenseMatrix
    */
  def iteratorToMatrix(iterator: Iterator[DenseVector[Double]])
  : DenseMatrix[Double] = {
    val (a, b) = iterator.duplicate
    val (c, d) = a.duplicate
    val length = b.length
    val attributes = c.next().length

    val matrix = new DenseMatrix[Double](length, attributes)
    for (i <- 0 until length) {
      matrix(i, ::) := d.next().t
    }
    matrix
  }

  /**
    * Converts Iterable of DenseVector to a DenseMatrix
    */
  def iterableToMatrix(iterable: Iterable[DenseVector[Double]])
  : DenseMatrix[Double] = {
    iteratorToMatrix(iterable.toIterator)
  }

  //////////////////////////////////////////////////////////////////////////////////////
  // DataStream
  //////////////////////////////////////////////////////////////////////////////////////

  /**
    * Converts DataStream of Iterable of DenseVector to DenseMatrix
    */
  def dataStreamToMatrices(inputStream: DataStream[Iterable[DenseVector[Double]]])
  : DataStream[DenseMatrix[Double]] = {
    inputStream.map(iterableToMatrix _)
  }

  /**
    * Converts DataStream of DenseVector to DenseMatrix
    * using a local count window per matrix.
    */
  def dataStreamToMatrices(inputStream: DataStream[DenseVector[Double]],
                           size: Long)
  : DataStream[DenseMatrix[Double]] = {
    dataStreamToMatrices(SubtaskWindowBatch.countBatch(inputStream, size))
  }

  /**
    * Converts DataStream of DenseVector to DenseMatrix
    * using a local time window per matrix.
    */
  def dataStreamToMatrices(inputStream: DataStream[DenseVector[Double]],
                           size: Time)
  : DataStream[DenseMatrix[Double]] = {
    dataStreamToMatrices(SubtaskWindowBatch.timeBatch(inputStream, size))
  }

  //////////////////////////////////////////////////////////////////////////////////////
  // DataSet
  //////////////////////////////////////////////////////////////////////////////////////

  /**
    * Converts DataSet of DenseVector to DenseMatrix using local data per matrix.
    */
  def dataSetToMatrices(inputStream: DataSet[DenseVector[Double]])
  : DataSet[DenseMatrix[Double]] = {
    inputStream
      .map(new PrependSubtaskID[DenseVector[Double]])
      .groupBy(_.subtaskID)
      .reduceGroup(x => {
        lazy val vectors = x.map(_.element)
        iteratorToMatrix(vectors)
      })
  }
}
