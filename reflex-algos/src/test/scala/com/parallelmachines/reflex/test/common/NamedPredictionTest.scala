package com.parallelmachines.reflex.test.common

import com.parallelmachines.reflex.common.enums.OpType
import org.json4s.jackson.JsonMethods._
import org.junit.Test
import org.mlpiper.datastructures.{ColumnEntry, NamedPrediction}
import org.scalatest.Matchers

class NamedPredictionTest extends Matchers {

  @Test
  def testReflexPredictionToJson(): Unit = {
    implicit val format = org.json4s.DefaultFormats
    val expectedRow =
      NamedPrediction(
        Option[Double](1),
        Array(
          ColumnEntry("c1", 1051750.0, OpType.CONTINUOUS)
        ), 2000, Some(1000))
    val json = expectedRow.toJson
    val jsonMap = parse(json).extract[Map[String, Any]]
    jsonMap.get("label").get should be("1.0")
    jsonMap.get("score").get should be("2000.0")
    jsonMap.get("timestamp").get should be("1000")
    jsonMap.get("data").get should be("(c1, 1051750.0)")
  }
}
