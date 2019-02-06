package org.apache.flink.streaming.scala.examples.functions.ml

import com.parallelmachines.reflex.common.InfoType._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.scala.examples.common.ml.PredictOperator
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterMap
import org.apache.flink.streaming.scala.examples.common.stats._
import org.apache.flink.streaming.scala.examples.functions.performance.TimedRichCoFlatMapFunction
import org.apache.flink.util.Collector

abstract class TimedRichCoFlatMapPredictOperator[Input, Model, Prediction](
                                                                            override protected var model: Model,
                                                                            params: ArgumentParameterMap)
  extends TimedRichCoFlatMapFunction[Input, Model, Prediction](params)
    with PredictOperator[Input, Prediction, Model] {

  private var modelUpdateCounterStat: GlobalAccumulator[Long] = _

  def updateStatAccumulator(modelUpdate: Long): Unit = {
    if (modelUpdateCounterStat != null) {
      modelUpdateCounterStat.localUpdate(modelUpdate)
    } else {
      // local policy will be to update according to sum whilst on globally, it would be just replacement for whatever we get from all subtask.
      modelUpdateCounterStat =
        StatInfo(
          statName = StatNames.ModelsUpdatedCounter,
          StatPolicy.SUM,
          StatPolicy.REPLACE
        )
          .toGlobalStat(modelUpdate,
            accumDataType = AccumData.getGraphType(modelUpdate),
            infoType = InfoType.General)
    }

    modelUpdateCounterStat.updateFlinkAccumulator(this.getRuntimeContext)
  }

  protected def internalFlatMap1Function(input: Input,
                                         out: Collector[Prediction]): Boolean = {
    val predictionAndSuccess = this.predict(input)
    out.collect(predictionAndSuccess._1)
    predictionAndSuccess._2
  }

  /** Wrapper function to internalFlatMap2Function */
  protected def modelUpdate(input: Model, out: Collector[Prediction]): Unit = {
    this.model = input

    this.updateStatAccumulator(modelUpdate = 1L)
  }

  override final def internalFlatMap2Function(input: Model,
                                              out: Collector[Prediction]): Unit = {
    modelUpdate(input, out)
  }

  override def open(config: Configuration): Unit = {
    super.open(config)
    this.prePredict()
  }
}
