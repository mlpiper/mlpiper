package org.mlpiper.stat.histogram.categorical

import com.parallelmachines.reflex.common.InfoType._
import org.apache.flink.streaming.scala.examples.common.stats._
import org.mlpiper.utils.ParsingUtils

import scala.collection.mutable

@SerialVersionUID(-3460383057266128369L)
class OverlapResult(val score: Map[String, Double],
                    val inputHistStream: mutable.Map[String, Histogram],
                    val contenderHistStream: mutable.Map[String, Histogram],
                    val outputHistogramsAlongScore: Boolean = false)
  extends Serializable {


  override def toString: String = {
    val mapOfOverlapScores: mutable.Map[String, Any] = mutable.Map[String, Any]()

    for (eachFeature <- score.keys) {
      mapOfOverlapScores(s"overlapScore_$eachFeature") = score(eachFeature).toString
    }

    if (outputHistogramsAlongScore) {
      inputHistStream.foreach(
        eachInputFeatureHist =>
          mapOfOverlapScores(s"input_${eachInputFeatureHist._1}") = eachInputFeatureHist._2.toGraphJsonable()
      )

      contenderHistStream.foreach(
        eachContenderFeatureHist =>
          mapOfOverlapScores(s"contender_${eachContenderFeatureHist._1}") = eachContenderFeatureHist._2.toGraphJsonable()
      )
    }

    ParsingUtils.iterableToJSON(mapOfOverlapScores)
  }
}

object OverlapResult {
  def getAccumulator(overlapResult: OverlapResult, infoType: InfoType, modelId: String)
  : GlobalAccumulator[OverlapResult] = {
    StatInfo(
      statName = StatNames.CategoricalOverlapScore,
      StatPolicy.REPLACE,
      StatPolicy.REPLACE
    ).toGlobalStat(overlapResult,
      accumDataType = AccumData.getGraphType(overlapResult.score),
      infoType = infoType,
      modelId = modelId)
  }
}
