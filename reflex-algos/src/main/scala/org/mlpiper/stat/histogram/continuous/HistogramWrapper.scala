package org.mlpiper.stat.histogram.continuous

import org.apache.flink.streaming.scala.examples.common.stats.BarGraphFormat
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.mlpiper.utils.ParsingUtils

import scala.collection.{immutable, mutable}

/**
  * Wrapper for the histogram map to provide a JSON toString method,
  * which is displayed in the web-ui.
  * Normalization as well as precision will remain true for wrappers
  *
  * @param histogram Histogram map for each index
  */
case class HistogramWrapper(histogram: mutable.Map[String, Histogram],
                            enableNormHistString: Boolean = true) {
  override def toString: String = {
    ParsingUtils.iterableToJSON(histogram.map(
      x => (x._1, x._2.setEnableNormHist(enableNormHistString).toGraphJsonable()))
    )
  }
}

object HistogramWrapper {
  def fromString(stringRep: String): mutable.Map[String, Histogram] = {
    //TODO this is a hack, that type for json Map is specified here, it should be defined per format
    val mapOfSerializedHist: immutable.Map[String, List[immutable.Map[String, Double]]] = Json(DefaultFormats).parse(stringRep).values.asInstanceOf[immutable.Map[String, List[immutable.Map[String, Double]]]]

    val featuredHist = mutable.Map[String, Histogram]()

    for (eachKey <- mapOfSerializedHist.keys) {
      val serializedHistogram = mapOfSerializedHist(eachKey)
      featuredHist(eachKey) = Histogram((BarGraphFormat.listMapToLinkedHashMap(serializedHistogram)))
    }

    featuredHist
  }
}
