package org.mlpiper.stat.algos

import breeze.linalg.DenseMatrix
import com.parallelmachines.reflex.common.InfoType
import org.apache.flink.streaming.scala.examples.common.stats.StatNames._
import org.apache.flink.streaming.scala.examples.common.stats._
import org.mlpiper.utils.ParsingUtils

import scala.collection.mutable

/** EvaluationStatistics will hold all statistics */
case class EvaluationStatistics(confusionMatrix: Option[ConfusionMatrixWrapper],
                                accuracy: Option[Double],
                                weightedPrecision: Option[Double],
                                weightedRecall: Option[Double],
                                recallPerclass: Option[RecallPerClassWrapper],
                                precisionPerclass: Option[PrecisionPerClassWrapper])

/** MatrixWrapper will hold all matrix */
class MatrixWrapper(val matrix: DenseMatrix[Double],
                    val rowsLabel: Array[String],
                    val colsLabel: Array[String]) extends Serializable {

  // format: map { "row0" : { "col1": value, "col2": value}}
  override def toString: String = {
    ParsingUtils.breezeDenseMatrixToJsonMap(matrix, Some(rowsLabel), Some(colsLabel))
  }

  def +(that: MatrixWrapper): MatrixWrapper = {
    new MatrixWrapper(
      matrix.+=(that.matrix),
      rowsLabel,
      colsLabel
    )
  }

  def getName(): String = {
    "matrix"
  }
}

case class ConfusionMatrixWrapper(override val matrix: DenseMatrix[Double],
                                  override val rowsLabel: Array[String],
                                  override val colsLabel: Array[String])
  extends MatrixWrapper(matrix, rowsLabel, colsLabel) {
  override def getName(): String = {
    ConfusionMatrixModelStat()
  }
}

object MatrixWrapper {

  /** Method is responsible for providing accumulator for MatrixWrapper */
  def getAccumulatorForMatrix(matrix: MatrixWrapper)
  : GlobalAccumulator[MatrixWrapper] = {
    new GlobalAccumulator[MatrixWrapper](
      name = matrix.getName(),
      // locally, confusion matrix will be replaced
      localMerge = (_: AccumulatorInfo[MatrixWrapper],
                    replacementMatrix: AccumulatorInfo[MatrixWrapper]) => {
        replacementMatrix
      },
      // globally, matrix will be added
      globalMerge = (x: AccumulatorInfo[MatrixWrapper],
                     y: AccumulatorInfo[MatrixWrapper]) => {
        AccumulatorInfo(
          value = x.value.+(y.value),
          count = x.count + y.count,
          accumModeType = x.accumModeType,
          accumGraphType = x.accumGraphType,
          name = x.name,
          infoType = x.infoType)
      },
      startingValue = matrix,
      accumDataType = AccumData.getGraphType(matrix.matrix),
      accumModeType = AccumMode.Instant,
      infoType = InfoType.InfoType.General
    )
  }
}

/** RecallPerClassWrapper will hold all Recall Score of Labels */
case class RecallPerClassWrapper(recallPerClassMap: mutable.Map[String, Double]) {

  override def toString: String = {
    ParsingUtils.iterableToJSON(recallPerClassMap.map(x => (x._1, x._2)))
  }

  def +(that: RecallPerClassWrapper): RecallPerClassWrapper = {
    val keys = recallPerClassMap.keys ++ that.recallPerClassMap.keys
    val keySet = keys.toSet

    val newRecallPerClassMap: mutable.Map[String, Double] = mutable.Map[String, Double]()

    for (eachKey <- keySet) {
      if (recallPerClassMap.contains(eachKey) && that.recallPerClassMap.contains(eachKey)) {
        newRecallPerClassMap(eachKey) = (recallPerClassMap(eachKey) + that.recallPerClassMap(eachKey)) / 2
      } else if (recallPerClassMap.contains(eachKey)) {
        newRecallPerClassMap(eachKey) = recallPerClassMap(eachKey)
      } else {
        newRecallPerClassMap(eachKey) = that.recallPerClassMap(eachKey)
      }
    }

    RecallPerClassWrapper(recallPerClassMap = newRecallPerClassMap)
  }
}

object RecallPerClassWrapper {
  /** Method is responsible for providing accumulator for RecallPerClassWrapper */
  def getAccumulator(recallPerClassWrapper: RecallPerClassWrapper)
  : GlobalAccumulator[RecallPerClassWrapper] = {
    new GlobalAccumulator[RecallPerClassWrapper](
      name = RecallPerClassPipelineStat(),
      // locally, RecallPerClassWrapper will be replaced
      localMerge = (_: AccumulatorInfo[RecallPerClassWrapper],
                    newRecallPerClassWrapper: AccumulatorInfo[RecallPerClassWrapper]) => {
        newRecallPerClassWrapper
      },
      globalMerge = (x: AccumulatorInfo[RecallPerClassWrapper],
                     y: AccumulatorInfo[RecallPerClassWrapper]) => {
        AccumulatorInfo(
          value = x.value.+(y.value),
          count = x.count + y.count,
          accumModeType = x.accumModeType,
          accumGraphType = x.accumGraphType,
          name = x.name,
          infoType = x.infoType)
      },
      startingValue = recallPerClassWrapper,
      accumDataType = AccumData.getGraphType(recallPerClassWrapper.recallPerClassMap),
      accumModeType = AccumMode.Instant,
      infoType = InfoType.InfoType.General
    )
  }
}

/** PrecisionPerClassWrapper will hold all Precision Score of Labels */
case class PrecisionPerClassWrapper(precisionPerClassMap: mutable.Map[String, Double]) {

  override def toString: String = {
    ParsingUtils.iterableToJSON(precisionPerClassMap.map(x => (x._1, x._2)))
  }

  def +(that: PrecisionPerClassWrapper): PrecisionPerClassWrapper = {
    val keys = precisionPerClassMap.keys ++ that.precisionPerClassMap.keys
    val keySet = keys.toSet

    val newPrecisionPerClassMap: mutable.Map[String, Double] = mutable.Map[String, Double]()

    for (eachKey <- keySet) {
      if (precisionPerClassMap.contains(eachKey) && that.precisionPerClassMap.contains(eachKey)) {
        newPrecisionPerClassMap(eachKey) = (precisionPerClassMap(eachKey) + that.precisionPerClassMap(eachKey)) / 2
      } else if (precisionPerClassMap.contains(eachKey)) {
        newPrecisionPerClassMap(eachKey) = precisionPerClassMap(eachKey)
      } else {
        newPrecisionPerClassMap(eachKey) = that.precisionPerClassMap(eachKey)
      }
    }

    PrecisionPerClassWrapper(precisionPerClassMap = newPrecisionPerClassMap)
  }
}

object PrecisionPerClassWrapper {
  /** Method is responsible for providing accumulator for PrecisionPerClassWrapper */
  def getAccumulator(precisionPerClassWrapper: PrecisionPerClassWrapper)
  : GlobalAccumulator[PrecisionPerClassWrapper] = {
    new GlobalAccumulator[PrecisionPerClassWrapper](
      name = PrecisionPerClassPipelineStat(),
      // locally, PrecisionPerClassWrapper will be replaced
      localMerge = (_: AccumulatorInfo[PrecisionPerClassWrapper],
                    newPrecisionPerClassWrapper: AccumulatorInfo[PrecisionPerClassWrapper]) => {
        newPrecisionPerClassWrapper
      },
      globalMerge = (x: AccumulatorInfo[PrecisionPerClassWrapper],
                     y: AccumulatorInfo[PrecisionPerClassWrapper]) => {
        AccumulatorInfo(
          value = x.value.+(y.value),
          count = x.count + y.count,
          accumModeType = x.accumModeType,
          accumGraphType = x.accumGraphType,
          name = x.name,
          infoType = x.infoType)
      },
      startingValue = precisionPerClassWrapper,
      accumDataType = AccumData.getGraphType(precisionPerClassWrapper.precisionPerClassMap),
      accumModeType = AccumMode.Instant,
      infoType = InfoType.InfoType.General)
  }
}
