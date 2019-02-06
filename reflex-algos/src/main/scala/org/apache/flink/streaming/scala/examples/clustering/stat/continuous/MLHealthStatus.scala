package org.apache.flink.streaming.scala.examples.clustering.stat.continuous

import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils

import scala.collection.immutable.ListMap
import scala.collection.mutable

@SerialVersionUID(3460383057266128976L)
class MLHealthStatus(val mapOfFeatureIDAndScore: mutable.Map[Int, Double],
                     val health: Boolean,
                     val listOfDivergedFeatures: mutable.ListBuffer[Int],
                     val threshold: Double) extends Serializable {
  def toJson(canaryDesc: Boolean = false): String = {
    val healthValString = "isHealthy" -> health

    val sortedListOfFeatureIDAndScore = ListMap(mapOfFeatureIDAndScore.toSeq.sortWith(_._2 > _._2): _*)

    val featureIDsValString = "attributes" -> sortedListOfFeatureIDAndScore.keys.mkString(", ")
    val scoresValString = "scores" -> sortedListOfFeatureIDAndScore.values.mkString(", ")

    val divergenceListString = "Diverged Attributes" -> listOfDivergedFeatures.toList.sortWith(_ < _).mkString(", ")

    val thresholdValString = "threshold" -> threshold

    val mapOfHistogramComparisionStatus: mutable.Map[String, Any] = mutable.Map[String, Any](
      featureIDsValString,
      scoresValString,
      thresholdValString,
      divergenceListString
    )

    val mapOfStatus: mutable.Map[String, Any] = mutable.Map[String, Any](
      healthValString,
      "histogramComparision" -> ParsingUtils.iterableToJSON(mapOfHistogramComparisionStatus)
    )

    var typeOfHealth = ""
    var description = ""

    if (canaryDesc) {
      typeOfHealth = "canaryPredictionComparision"

      description = "Canary Health Is Good!"

      if (listOfDivergedFeatures.nonEmpty) {
        description = s"Canary prediction distributions diverge with a threshold set to ${threshold}."
      }
    } else {
      typeOfHealth = "histogramComparision"

      description = "Health Is Good!"

      if (listOfDivergedFeatures.nonEmpty) {
        description = s"Health violation occurred for attributes ${divergenceListString._2} as they exceeded threshold ${threshold}."
      }
    }

    mapOfStatus("type") = typeOfHealth
    mapOfStatus("description") = description

    ParsingUtils.iterableToJSON(mapOfStatus)
  }
}
