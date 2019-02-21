package org.apache.flink.streaming.scala.examples.clustering.stat.continuous

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

class GenerateMLHealthStatus(val threshold: Double)
  extends RichFlatMapFunction[Array[Double], MLHealthStatus] {

  override def flatMap(scoreArray: Array[Double], out: Collector[MLHealthStatus]): Unit = {
    val mapOfFeatureIDAndScore: mutable.Map[Int, Double] = mutable.Map[Int, Double]()

    scoreArray
      .zipWithIndex
      .foreach(
        x =>
          mapOfFeatureIDAndScore(x._2) = BigDecimal.decimal(x._1).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
      )

    val listOfDivergedFeatures = mutable.ListBuffer.empty[Int]
    var health = true

    for (eachFeature <- scoreArray.indices) {
      if (scoreArray(eachFeature) >= threshold) {
        listOfDivergedFeatures += (eachFeature)
        health = false
      }
    }

    val healthStatus: MLHealthStatus = new MLHealthStatus(
      mapOfFeatureIDAndScore = mapOfFeatureIDAndScore,
      health = health,
      listOfDivergedFeatures = listOfDivergedFeatures,
      threshold = threshold
    )

    out.collect(healthStatus)
  }
}
