package org.apache.flink.streaming.scala.examples.functions.conversion

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.scala.examples.clustering.math.{ReflexColumnEntry, ReflexNamedVector}
import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/** This class converts the preprocesses a dataset */
class JsonToNamedVectorFlatMap extends RichFlatMapFunction[String, ReflexNamedVector] {

  private val logger = LoggerFactory.getLogger(getClass)

  override def flatMap(value: String, out: Collector[ReflexNamedVector]): Unit = {

    val preprocessedVector: Option[ReflexNamedVector] = ParsingUtils.jsonToNamedVector(value)
    if (preprocessedVector.isEmpty) {
      logger.error(raw"Processing failed ${value}")
    } else {
      out.collect(preprocessedVector.get)
    }
  }
}
