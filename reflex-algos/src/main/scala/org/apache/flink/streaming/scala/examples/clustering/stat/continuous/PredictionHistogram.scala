package org.apache.flink.streaming.scala.examples.clustering.stat.continuous

import com.parallelmachines.reflex.common.{HealthType, InfoType}
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.scala.examples.clustering.stat.utils.PrependElementID
import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils
import org.apache.flink.streaming.scala.examples.common.stats._
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import scala.collection.{immutable, mutable}

case class PredictionHistogram(ID: Long, featuredHist: mutable.Map[String, Histogram])
  extends Serializable {
  override def equals(that: scala.Any): Boolean = {
    this.toString.equals(that.toString)
  }
}

object PredictionHistogram {
  val ID = "id"
  val Name = "name"
  val Value = "value"
  val infoName = HealthType.PredictionHistogramHealth.toString

  val idAndFeaturedHistDivider = "-"

  def generateIDedFeaturedHist(featuredHistogram: DataStream[mutable.Map[String, Histogram]]): DataStream[PredictionHistogram] = {
    featuredHistogram
      .countWindowAll(1)
      .apply(new PrependElementID[mutable.Map[String, Histogram]])
      .map(x => PredictionHistogram(ID = x.elementID, featuredHist = x.element))
  }

  def toIDedFeatureHistogramJSON(idedFeaturedHistogram: PredictionHistogram): String = {
    val customMap = mutable.Map[String, String](ID -> idedFeaturedHistogram.ID.toString)

    val histWrapper = HistogramWrapper(idedFeaturedHistogram.featuredHist,
      enableNormHistString = false)

    val accumHist = AccumulatorInfo[HistogramWrapper](
      value = histWrapper,
      accumGraphType = AccumData.BarGraph,
      accumModeType = AccumMode.Instant,
      customMap = Some(customMap),
      name = infoName,
      infoType = InfoType.InfoType.Health
    )

    val accumMap = mutable.Map[String, String](Name -> infoName, Value -> accumHist.toString)
    ParsingUtils.iterableToJSON(accumMap)
  }

  def fromIDedFeatureHistogramJSON(idedFeaturedHistogramString: String): PredictionHistogram = {
    val mapOfIDAndFHist: immutable.Map[String, String] = Json(DefaultFormats).parse(idedFeaturedHistogramString).values.asInstanceOf[immutable.Map[String, String]]

    require(mapOfIDAndFHist(Name) == infoName)

    val valueString = mapOfIDAndFHist(Value)
    val mapOfData: immutable.Map[String, String] = Json(DefaultFormats).parse(valueString).values.asInstanceOf[immutable.Map[String, String]]

    val histString = mapOfData(AccumulatorInfoJsonHeaders.DataKey)
    val id = mapOfData(ID)

    val hist = HistogramWrapper.fromString(histString)

    PredictionHistogram(id.toLong, hist)
  }
}
