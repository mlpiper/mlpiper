package org.mlpiper.stat.algos

import com.parallelmachines.reflex.common.InfoType._
import org.mlpiper.stats._
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame


/**
  * Class is responsible for updating accumulator state of all kind of evaluation statistics -
  * rmse"(default): root mean squared error
  *  - "mse": mean squared error
  *  - "r2": R^2^ metric
  *  - "mae": mean absolute error
  *  - "ev": explained variance
  */
class RegressionEvaluation(evaluationRegStatistics: EvaluationRegStatistics,
                               sparkContext: org.apache.spark.SparkContext) {


  private var globalStatForRmse: GlobalAccumulator[Double] = _

  /** Method will update RMSE in associated accumulator */
  private def updateStatAccumulatorForRmse(rmse: Double): Unit = {
    if (globalStatForRmse != null) {
      globalStatForRmse.localUpdate(rmse)
    } else {
      globalStatForRmse = StatInfo(statName = StatNames.PredictionRmse,
        StatPolicy.REPLACE,
        StatPolicy.AVERAGE)
        .toGlobalStat(rmse,
          accumDataType = AccumData.getGraphType(rmse),
          infoType = InfoType.General)

    }
    globalStatForRmse.updateSparkAccumulator(sparkContext)
  }


  private var globalStatForMse: GlobalAccumulator[Double] = _

  /** Method will update MSE in associated accumulator */
  private def updateStatAccumulatorForMse(mse: Double): Unit = {
    if (globalStatForMse != null) {
      globalStatForMse.localUpdate(mse)
    } else {
      globalStatForMse
        = StatInfo(statName = StatNames.PredictionMse,
        StatPolicy.REPLACE,
        StatPolicy.AVERAGE)
        .toGlobalStat(mse,
          accumDataType = AccumData.getGraphType(mse),
          infoType = InfoType.General)

    }
    globalStatForMse.updateSparkAccumulator(sparkContext)
  }

  private var globalStatForR2: GlobalAccumulator[Double] = _

  /** Method will update R2 in associated accumulator */
  private def updateStatAccumulatorForR2(r2: Double): Unit = {
    if (globalStatForR2 != null) {
      globalStatForR2.localUpdate(r2)
    } else {
      globalStatForR2
        = StatInfo(statName = StatNames.PredictionR2,
        StatPolicy.REPLACE,
        StatPolicy.AVERAGE)
        .toGlobalStat(r2,
          accumDataType = AccumData.getGraphType(r2),
          infoType = InfoType.General)

    }
    globalStatForR2.updateSparkAccumulator(sparkContext)
  }

  private var globalStatForMae: GlobalAccumulator[Double] = _

  /** Method will update MAE in associated accumulator */
  private def updateStatAccumulatorForMae(mae: Double): Unit = {
    if (globalStatForMae != null) {
      globalStatForMae.localUpdate(mae)
    } else {
      globalStatForMae
        = StatInfo(statName = StatNames.PredictionMae,
        StatPolicy.REPLACE,
        StatPolicy.AVERAGE)
        .toGlobalStat(mae,
          accumDataType = AccumData.getGraphType(mae),
          infoType = InfoType.General)

    }
    globalStatForMae.updateSparkAccumulator(sparkContext)
  }


  private var globalStatForEv: GlobalAccumulator[Double] = _

  /** Method will update EV in associated accumulator */
  private def updateStatAccumulatorForEv(ev: Double): Unit = {
    if (globalStatForEv != null) {
      globalStatForEv.localUpdate(ev)
    } else {
      globalStatForEv
        = StatInfo(statName = StatNames.PredictionEv,
        StatPolicy.REPLACE,
        StatPolicy.AVERAGE)
        .toGlobalStat(ev,
          accumDataType = AccumData.getGraphType(ev),
          infoType = InfoType.General)

    }
    globalStatForEv.updateSparkAccumulator(sparkContext)
  }


  /** Method will be called whenever state of all stats needs to be updated. */
  def updateStatAccumulator(): Unit = {

    // update RMSE accumulator
    if (evaluationRegStatistics.rmse.isDefined) {
      this.updateStatAccumulatorForRmse(evaluationRegStatistics.rmse.get)
    }
    // update MSE accumulator
    if (evaluationRegStatistics.mse.isDefined) {
      this.updateStatAccumulatorForMse(evaluationRegStatistics.mse.get)
    }
    // update R2 accumulator
    if (evaluationRegStatistics.r2.isDefined) {
      this.updateStatAccumulatorForR2(evaluationRegStatistics.r2.get)
    }
    // update MAE accumulator
    if (evaluationRegStatistics.mae.isDefined) {
      this.updateStatAccumulatorForMae(evaluationRegStatistics.mae.get)
    }

    // update EV accumulator
    if (evaluationRegStatistics.ev.isDefined) {
      this.updateStatAccumulatorForEv(evaluationRegStatistics.ev.get)
    }
  }
}

/**
  * Object [[RegressionEvaluation]] will be basically responsible for creating evaluation statistics
  * from predicted and actual labels
  */
object RegressionEvaluation {

  var regressionMetrics: RegressionMetrics = _


  /** Method will give RMSE of predictions of algorithm */
  private def getRmse: Option[Double] = {
    require(regressionMetrics != null, "Data has not been loaded in regressionMetrics")

    Some(regressionMetrics.rootMeanSquaredError)
  }

  /** Method will give MSE of predictions of algorithm */
  private def getMse: Option[Double] = {
    require(regressionMetrics != null, "Data has not been loaded in regressionMetrics")

    Some(regressionMetrics.meanSquaredError)
  }

  /** Method will give R2 of predictions of algorithm */
  private def getR2: Option[Double] = {
    require(regressionMetrics != null, "Data has not been loaded in regressionMetrics")

    Some(regressionMetrics.r2)
  }

  /** Method will give MAE of predictions of algorithm */
  private def getMae: Option[Double] = {
    require(regressionMetrics != null, "Data has not been loaded in regressionMetrics")

    Some(regressionMetrics.meanAbsoluteError)
  }

  /** Method will give EV of predictions of algorithm */
  private def getEv: Option[Double] = {
    require(regressionMetrics != null, "Data has not been loaded in regressionMetrics")

    Some(regressionMetrics.explainedVariance)
  }


  /**
    * Method is responsible for creating RegressionMetrics from RDD of tuples of double
    * First element in tuple represents predicted label whilst second is actual label of data
    */
  private def loadRegressionMetrics(predictionAndActualLabel: RDD[(Double, Double)]): RegressionMetrics = {
    regressionMetrics = new RegressionMetrics(predictionAndActualLabel)
    regressionMetrics
  }

  /**
    * Method is responsible for creating EvaluationRegStatistics from RDD of tuple of doubles
    * First element in tuple represents predicted label whilst second is actual label of data
    */
  def generateStats(predictionAndActualLabel: RDD[(Double, Double)])
  : EvaluationRegStatistics = {
    loadRegressionMetrics(predictionAndActualLabel)

    val rmse: Option[Double] = getRmse

    val mse: Option[Double] = getMse

    val r2: Option[Double] = getR2

    val mae: Option[Double] = getMae

    val ev: Option[Double] = getEv


    EvaluationRegStatistics(
      rmse = rmse,
      mse = mse,
      r2 = r2,
      mae = mae,
      ev = ev
    )
  }

  /**
    * Method is responsible for creating EvaluationRegStatistics from Dataframe.
    * Method needs predictedLabel and actualLabel index in frame.
    */
  def generateStats(dataFrame: DataFrame,
                    indexOfPredictedL: Int = 0,
                    indexOfActualL: Int = 1): EvaluationRegStatistics = {
    val predictionAndActualLabel: RDD[(Double, Double)] =
      dataFrame
        .rdd
        .map(
          GeneralEvaluation.rowToTupleDoubles(_,
            indexOfFirstElement = indexOfPredictedL,
            indexOfSecondElement = indexOfActualL)
        )

    generateStats(predictionAndActualLabel)
  }
}

case class EvaluationRegStatistics(rmse: Option[Double],
                                    mse: Option[Double],
                                    r2: Option[Double],
                                    mae: Option[Double],
                                    ev: Option[Double])
