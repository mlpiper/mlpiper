package org.apache.flink.streaming.scala.examples.common.ml.evaluation

import breeze.linalg.{DenseMatrix, DenseVector, sum}
import com.parallelmachines.reflex.common.InfoType._
import org.apache.flink.streaming.scala.examples.common.stats._
import org.apache.flink.streaming.scala.examples.functions.math.matrix.BreezeDenseVectorsToMatrix
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Class is responsible for updating accumulator state of all kind of evaluation statistics - confusion matrix, f1score, etc.
  */
class ClassificationEvaluation(evaluationStatistics: EvaluationStatistics,
                               sparkContext: org.apache.spark.SparkContext) {
  private var globalStatForConfusionMatrix: GlobalAccumulator[ConfusionMatrixWrapper] = _

  /** Method will update confion matrix in associated accumulator */
  private def updateStatAccumulatorForConfusionMatrix(confusionMatrix: ConfusionMatrixWrapper): Unit = {
    if (globalStatForConfusionMatrix != null) {
      globalStatForConfusionMatrix.localUpdate(confusionMatrix)
    } else {
      globalStatForConfusionMatrix = MatrixWrapper.getAccumulatorForMatrix(confusionMatrix)
        .asInstanceOf[GlobalAccumulator[ConfusionMatrixWrapper]]
    }

    globalStatForConfusionMatrix.updateSparkAccumulator(sparkContext)
  }

  private var globalStatForAccuracy: GlobalAccumulator[Double] = _

  /** Method will update accuracy in associated accumulator */
  private def updateStatAccumulatorForAccuracy(accuracy: Double): Unit = {
    if (globalStatForAccuracy != null) {
      globalStatForAccuracy.localUpdate(accuracy)
    } else {
      globalStatForAccuracy
        = StatInfo(statName = StatNames.PredictionAccuracy,
        StatPolicy.REPLACE,
        StatPolicy.AVERAGE)
        .toGlobalStat(accuracy,
          accumDataType = AccumData.getGraphType(accuracy),
          infoType = InfoType.General)

    }
    globalStatForAccuracy.updateSparkAccumulator(sparkContext)
  }

  private var globalStatForWeightedPrecision: GlobalAccumulator[Double] = _

  /** Method will update weighted precision in associated accumulator */
  private def updateStatAccumulatorForWeightedPrecision(weightedPrecision: Double): Unit = {
    if (globalStatForWeightedPrecision != null) {
      globalStatForWeightedPrecision.localUpdate(weightedPrecision)
    } else {
      globalStatForWeightedPrecision
        = StatInfo(statName = StatNames.WeightedPrecision,
        StatPolicy.REPLACE,
        StatPolicy.AVERAGE)
        .toGlobalStat(weightedPrecision,
          accumDataType = AccumData.getGraphType(weightedPrecision),
          infoType = InfoType.General)
    }
    globalStatForWeightedPrecision.updateSparkAccumulator(sparkContext)
  }

  private var globalStatForWeightedRecall: GlobalAccumulator[Double] = _

  /** Method will update weighted recall in associated accumulator */
  private def updateStatAccumulatorForWeightedRecall(weightedRecall: Double): Unit = {
    if (globalStatForWeightedRecall != null) {
      globalStatForWeightedRecall.localUpdate(weightedRecall)
    } else {
      globalStatForWeightedRecall
        = StatInfo(statName = StatNames.WeightedRecall,
        StatPolicy.REPLACE,
        StatPolicy.AVERAGE)
        .toGlobalStat(weightedRecall,
          accumDataType = AccumData.getGraphType(weightedRecall),
          infoType = InfoType.General)
    }
    globalStatForWeightedRecall.updateSparkAccumulator(sparkContext)
  }

  private var globalStatForRecallLabel: GlobalAccumulator[RecallPerClassWrapper] = _

  /** Method will update recall Per Class in associated accumulator */
  private def updateStatAccumulatorFoRecallPerLabel(recallPerClass: RecallPerClassWrapper): Unit = {
    if (globalStatForRecallLabel != null) {
      globalStatForRecallLabel.localUpdate(recallPerClass)
    } else {
      globalStatForRecallLabel = RecallPerClassWrapper.getAccumulator(recallPerClass)

    }

    globalStatForRecallLabel.updateSparkAccumulator(sparkContext)
  }

  private var globalStatForPrecisionPerLabel: GlobalAccumulator[PrecisionPerClassWrapper] = _

  /** Method will update precision Per Class in associated accumulator */
  private def updateStatAccumulatorFoPrecisionPerLabel(precisionPerClass: PrecisionPerClassWrapper): Unit = {
    if (globalStatForPrecisionPerLabel != null) {
      globalStatForPrecisionPerLabel.localUpdate(precisionPerClass)
    } else {
      globalStatForPrecisionPerLabel = PrecisionPerClassWrapper.getAccumulator(precisionPerClass)
    }

    globalStatForPrecisionPerLabel.updateSparkAccumulator(sparkContext)
  }

  /** Method will be called whenever state of all stats needs to be updated. */
  def updateStatAccumulator(): Unit = {
    // update confusion matrix accumulator
    if (evaluationStatistics.confusionMatrix.isDefined) {
      this.updateStatAccumulatorForConfusionMatrix(evaluationStatistics.confusionMatrix.get)
    }
    // update accuracy accumulator
    if (evaluationStatistics.accuracy.isDefined) {
      this.updateStatAccumulatorForAccuracy(evaluationStatistics.accuracy.get)
    }
    // update weighted precision accumulator
    if (evaluationStatistics.weightedPrecision.isDefined) {
      this.updateStatAccumulatorForWeightedPrecision(evaluationStatistics.weightedPrecision.get)
    }
    // update weighted recall accumulator
    if (evaluationStatistics.weightedRecall.isDefined) {
      this.updateStatAccumulatorFoRecallPerLabel(evaluationStatistics.recallPerclass.get)
    }
    // update recall per label accumulator
    if (evaluationStatistics.recallPerclass.isDefined) {
      this.updateStatAccumulatorForWeightedRecall(evaluationStatistics.weightedRecall.get)
    }
    // update precision per label accumulator
    if (evaluationStatistics.precisionPerclass.isDefined) {
      this.updateStatAccumulatorFoPrecisionPerLabel(evaluationStatistics.precisionPerclass.get)
    }
  }
}

/**
  * Object [[ClassificationEvaluation]] will be basically responsible for creating evaluation statistics from predicted and actual labels
  */
object ClassificationEvaluation {

  var multiclassMetrics: MulticlassMetrics = _


  /** Method will generate row wise error list of double and columnly total */
  private def generateStatForConfusionMatrix(confusionMatrix: DenseMatrix[Double])
  : (ListBuffer[Double], ListBuffer[Double]) = {
    // rows will represent actual labels
    val rows = confusionMatrix.rows
    // cols will represent predicted labels
    val cols = confusionMatrix.cols

    require(rows == cols, "Confusion matrix's rows and cols do not match")

    // list will hold error in predicting correct labels
    val rowsError = new ListBuffer[Double]()
    val colsTotalPrediction = new ListBuffer[Double]()

    var totalPredictions = 0.0
    var wrongPredictions = 0.0
    for (eachRowLabel <- 0 until rows) {
      val sumOfRow = sum(confusionMatrix(eachRowLabel, ::).inner)

      val valueOfCorrectClassification = confusionMatrix.valueAt(eachRowLabel, eachRowLabel)

      val missClassified = sumOfRow - valueOfCorrectClassification
      val errorForRowLabel = missClassified / sumOfRow

      rowsError += errorForRowLabel

      totalPredictions += sumOfRow
      wrongPredictions += missClassified
    }

    rowsError += wrongPredictions / totalPredictions

    for (eachColLabel <- 0 until cols) {
      val sumOfColumn = sum(confusionMatrix(::, eachColLabel))

      colsTotalPrediction += sumOfColumn
    }

    (rowsError, colsTotalPrediction)
  }

  /**
    * Method will create wrapper of confusion matrix along with extra information like row wise error and columnly total
    * SparkML used MLLib to create confusion matrix and thus it looses labels. We need to explicitly handle them
    */
  private def getConfusionMatrixWrapper(labelTransformationDetails: Option[Map[Double, String]])
  : Option[ConfusionMatrixWrapper] = {
    require(multiclassMetrics != null, "Data has not been loaded in multiclassMetrics")

    // getting confusion matrix from multiclassMetrics.
    // This will be same for both multiclass as well as binaryclass classification
    val confusionMatrix: Matrix =
    multiclassMetrics
      .confusionMatrix

    val doubleLabels: Array[Double] = multiclassMetrics.labels

    // creating confusion matrix in form of [[BreezeDenseMatrix]]
    val confusionIterator: Iterator[DenseVector[Double]] =
      confusionMatrix
        .rowIter
        .map(_.toArray)
        .map(DenseVector(_))

    val confusionDenseMatrix = BreezeDenseVectorsToMatrix.iteratorToMatrix(confusionIterator)

    val statOfConfusionMatrix = generateStatForConfusionMatrix(confusionMatrix = confusionDenseMatrix)

    val rowsError = statOfConfusionMatrix._1
    val colsTotalPrediction = statOfConfusionMatrix._2

    val confusionWithColsPred = DenseMatrix.vertcat(confusionDenseMatrix, DenseMatrix(colsTotalPrediction))
    val confusionWithRowErrorAndColsPred = DenseMatrix.horzcat(confusionWithColsPred, DenseMatrix(rowsError).t)

    val rowsLabel = doubleLabels.map(eachLabel => {
      val transformedLabel: String = if (labelTransformationDetails.isDefined) labelTransformationDetails.get(eachLabel) else eachLabel.toString

      transformedLabel
    }) :+ "Total"

    val colsLabel = doubleLabels.map(eachLabel => {
      val transformedLabel: String = if (labelTransformationDetails.isDefined) labelTransformationDetails.get(eachLabel) else eachLabel.toString

      transformedLabel
    }) :+ "Error"

    Some(ConfusionMatrixWrapper(confusionWithRowErrorAndColsPred, rowsLabel = rowsLabel, colsLabel = colsLabel))
  }

  /** Method will give Accuracy of prediction capability of algorithm */
  private def getAccuracy: Option[Double] = {
    require(multiclassMetrics != null, "Data has not been loaded in multiclassMetrics")

    Some(multiclassMetrics.accuracy)
  }

  /** Method will give Weighted Precision of prediction capability of algorithm */
  private def getWeightedPrecision: Option[Double] = {
    require(multiclassMetrics != null, "Data has not been loaded in multiclassMetrics")

    Some(multiclassMetrics.weightedPrecision)
  }

  /** Method will give Weighted Recall of prediction capability of algorithm */
  private def getWeightedRecall: Option[Double] = {
    require(multiclassMetrics != null, "Data has not been loaded in multiclassMetrics")

    Some(multiclassMetrics.weightedRecall)
  }

  /** Method will give Recall per Class of prediction capability of algorithm */
  private def getRecallPerClass(labelTransformationDetails: Option[Map[Double, String]]): Option[RecallPerClassWrapper] = {
    require(multiclassMetrics != null, "Data has not been loaded in multiclassMetrics")

    val mapOfLabelAndRecall: mutable.Map[String, Double] = mutable.Map[String, Double]()
    multiclassMetrics.labels.foreach(eachLabel => {
      val transformedLabel: String = if (labelTransformationDetails.isDefined) labelTransformationDetails.get(eachLabel) else eachLabel.toString
      mapOfLabelAndRecall(transformedLabel) = multiclassMetrics.recall(eachLabel)
    })

    Some(RecallPerClassWrapper(mapOfLabelAndRecall))
  }

  /** Method will give Precision per Class of prediction capability of algorithm */
  private def getPrecisionPerClass(labelTransformationDetails: Option[Map[Double, String]]): Option[PrecisionPerClassWrapper] = {
    require(multiclassMetrics != null, "Data has not been loaded in multiclassMetrics")

    val mapOfLabelAndPrecision: mutable.Map[String, Double] = mutable.Map[String, Double]()
    multiclassMetrics.labels.foreach(eachLabel => {
      val transformedLabel: String = if (labelTransformationDetails.isDefined) labelTransformationDetails.get(eachLabel) else eachLabel.toString

      mapOfLabelAndPrecision(transformedLabel) = multiclassMetrics.precision(eachLabel)
    })

    Some(PrecisionPerClassWrapper(mapOfLabelAndPrecision))
  }

  /**
    * Method is responsible for creating MulticlassMetrics from RDD of tuples of double
    * First element in tuple represents predicted label whilst second is actual label of data
    */
  private def loadMulticlassMetrics(predictionAndActualLabel: RDD[(Double, Double)]): MulticlassMetrics = {
    multiclassMetrics = new MulticlassMetrics(predictionAndActualLabel)
    multiclassMetrics
  }

  /**
    * Method is responsible for creating EvaluationStatistics from RDD of tuple of doubles
    * First element in tuple represents predicted label whilst second is actual label of data
    */
  def generateStats(predictionAndActualLabel: RDD[(Double, Double)],
                    labelTransformationDetails: Option[Map[Double, String]])
  : EvaluationStatistics = {
    loadMulticlassMetrics(predictionAndActualLabel)

    // all stats need to work with labelTransformationDetails as mllib works with doubles and we might have encounter transformation for them.

    val confusionMatrixWrapper: Option[ConfusionMatrixWrapper] =
      getConfusionMatrixWrapper(labelTransformationDetails = labelTransformationDetails)

    val accuracy: Option[Double] = getAccuracy

    val weightedPrecision: Option[Double] = getWeightedPrecision

    val weightedRecall: Option[Double] = getWeightedRecall

    val recallPerclass = getRecallPerClass(labelTransformationDetails)

    val precisionPerclass = getPrecisionPerClass(labelTransformationDetails)

    EvaluationStatistics(
      confusionMatrix = confusionMatrixWrapper,
      accuracy = accuracy,
      weightedPrecision = weightedPrecision,
      weightedRecall = weightedRecall,
      recallPerclass = recallPerclass,
      precisionPerclass = precisionPerclass
    )
  }

  /**
    * Method is responsible for creating EvaluationStatistics from Dataframe. Method needs predictedLabel and actualLabel index in frame.
    */
  def generateStats(dataFrame: DataFrame,
                    indexOfPredictedL: Int = 0,
                    indexOfActualL: Int = 1,
                    labelTransformationDetails: Option[Map[Double, String]]): EvaluationStatistics = {
    val predictionAndActualLabel: RDD[(Double, Double)] =
      dataFrame
        .rdd
        .map(
          GeneralEvaluation.rowToTupleDoubles(_,
            indexOfFirstElement = indexOfPredictedL,
            indexOfSecondElement = indexOfActualL)
        )

    generateStats(predictionAndActualLabel, labelTransformationDetails)
  }
}


/**
  * Object [[GeneralEvaluation]] will be basically responsible for creating general evaluation
  * functions
  */
object GeneralEvaluation {

  /**
    * Method is resposible for converting row to tuple of elements
    *
    * @param dataRow              Row of data in format of [[org.apache.spark.sql.Row]]
    * @param indexOfFirstElement  Element's index in row which will be first element in new created tuple
    * @param indexOfSecondElement Element's index in row which will be second element in new created tuple
    */
  def rowToTupleDoubles(dataRow: Row,
                        indexOfFirstElement: Int,
                        indexOfSecondElement: Int)
  : (Double, Double) = {
    require(dataRow.length == 2, s"Row cannot be converted to tuple of two doubles as size of row is ${dataRow.length}")

    var firstElement = 0.0
    var secondElement = 0.0

    try {
      firstElement = dataRow.get(indexOfFirstElement).asInstanceOf[java.lang.Number].doubleValue()
    }
    catch {
      case e: java.lang.ClassCastException =>
        firstElement = dataRow.getInt(indexOfFirstElement).toDouble
    }

    try {
      secondElement = dataRow.get(indexOfSecondElement).asInstanceOf[java.lang.Number].doubleValue()
    }
    catch {
      case e: java.lang.ClassCastException =>
        secondElement = dataRow.getInt(indexOfSecondElement).toDouble
    }

    (firstElement, secondElement)
  }

}
