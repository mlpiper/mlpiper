package org.mlpiper.stat.heatmap.continuous

import com.parallelmachines.reflex.common.InfoType
import org.mlpiper.stats._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mlpiper.datastructures.NamedVector
import org.mlpiper.stat.heatmap.continuous.localgenerator.NormalizedMeanHeatMapHelper
import org.mlpiper.utils.ParsingUtils
import org.slf4j.LoggerFactory

/**
  * Delegates computation to the specific heatMap object based on the input method name.
  * HeatMap calculation supports double windowing feature also.
  * So after generating local windowing heatMap values, based on enable flag, computation may further be delegated to global heatMap method.
  */
object HeatMap {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Method is responsible for creating global accumulator object for heatMapValues.
    */
  def getAccumulator(startingHeatMap: HeatMapValues)
  : GlobalAccumulator[HeatMapValues] = {
    new GlobalAccumulator[HeatMapValues](
      table = StatTable.DATA_HEATMAP,
      localMerge = (_: AccumulatorInfo[HeatMapValues],
                    newHeatMap: AccumulatorInfo[HeatMapValues]) => {
        newHeatMap
      },
      globalMerge = (x: AccumulatorInfo[HeatMapValues],
                     y: AccumulatorInfo[HeatMapValues]) => {
        AccumulatorInfo(
          value = x.value.+(y.value),
          count = x.count + y.count,
          accumModeType = x.accumModeType,
          accumGraphType = x.accumGraphType,
          name = x.name,
          infoType = x.infoType)
      },
      startingValue = startingHeatMap,
      accumDataType = AccumData.Heatmap,
      accumModeType = AccumMode.TimeSeries,
      infoType = InfoType.InfoType.General
    )
  }

  //////////////////////////////////////////////////////////////////////////////////////
  // Spark RDD
  //////////////////////////////////////////////////////////////////////////////////////
  /**
    * Generate the heatMaps for the input RDD of named vectors.
    *
    * @param rddOfNamedVec batch of named vectors.
    * @param env           Spark context
    * @return A stream of heatmap values calculated on each batches.
    */
  def createHeatMap(rddOfNamedVec: RDD[NamedVector],
                    env: SparkContext): Option[HeatMapValues] = {
    val rddOfContinuousVec = rddOfNamedVec.map(_.toContinuousNamedVector(dropNa = true)).filter(_.vector.nonEmpty)

    val continuousNamedVecIterator = rddOfContinuousVec.collect()
    if (!continuousNamedVecIterator.isEmpty) {
      val heatMap = NormalizedMeanHeatMapHelper.generateHeatMap(continuousNamedVecIterator)

      val heatmapAcc = getAccumulator(heatMap)
      heatmapAcc.updateSparkAccumulator(env)

      Some(heatMap)
    } else {
      logger.warn("HeatMap is not generated as no continuous features were found")
      None
    }
  }
}

case class HeatMapValues(heatMapValue: Map[String, Double], preciseValue: Boolean = true, globalParams: Option[GlobalParams] = None)
  extends Serializable {
  override def toString: String = {
    if (preciseValue) {
      val heatMapPrecise = heatMapValue.map(eachValue => (eachValue._1, BigDecimal.decimal(eachValue._2).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble))

      ParsingUtils.iterableToJSON(heatMapPrecise)
    }
    else {
      ParsingUtils.iterableToJSON(heatMapValue)
    }
  }


  def +(that: HeatMapValues): HeatMapValues = {
    val heatMapAddedValue = this.heatMapValue.keys.map(x => (x, (this.heatMapValue(x) + that.heatMapValue(x)) / 2.0)).toMap

    HeatMapValues(heatMapAddedValue, this.preciseValue, this.globalParams)
  }
}

case class GlobalParams(params1: Map[String, Double], params2: Map[String, Double]) extends Serializable
