package org.mlpiper.stat.algos.pca

import breeze.linalg.{DenseVector => BreezeDenseVector}
import com.parallelmachines.reflex.common.InfoType._
import org.mlpiper.stats._
import org.mlpiper.utils.ParsingUtils

case class ExplainedVarianceStat(explainedVariance: BreezeDenseVector[Double])
  extends Serializable {
  override def toString: String = {
    ParsingUtils.breezeDenseVectorToJsonMap(explainedVariance)
  }
}

object ExplainedVarianceStat {
  def getAccumulator(explainedVarianceWrapper: ExplainedVarianceStat)
  : GlobalAccumulator[ExplainedVarianceStat] = {
    StatInfo(
      StatNames.ExplainedVariance,
      StatPolicy.REPLACE,
      StatPolicy.REPLACE
    ).toGlobalStat(
      explainedVarianceWrapper,
      accumDataType = AccumData.getGraphType(explainedVarianceWrapper.explainedVariance),
      infoType = InfoType.General
    )
  }
}
