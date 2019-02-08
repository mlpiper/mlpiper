package org.mlpiper.stat.heatmap.continuous

import com.parallelmachines.reflex.common.InfoType
import org.apache.flink.api.common.functions.{AbstractRichFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala.function.RichAllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow, Window}
import org.apache.flink.streaming.scala.examples.common.stats._
import org.apache.flink.streaming.scala.examples.flink.utils.{IDElement, PrependElementID, StreamsCombiner}
import org.apache.flink.streaming.scala.examples.flink.utils.functions.map.SubtaskElement
import org.apache.flink.streaming.scala.examples.flink.utils.functions.window.SubtaskWindowBatch
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.mlpiper.datastructures.NamedVector
import org.mlpiper.stat.heatmap.continuous.globalgenerator.{MinMaxHeatMap, MinMaxParamsGenerator, StandardScaleHeatMap, StandardScaleParamsGenerator}
import org.mlpiper.stat.heatmap.continuous.localgenerator.{MeanHeatMap, NormalizedMeanHeatMap, NormalizedMeanHeatMapHelper}
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

  //////////////////////////////////////////////////////////////////////////////////////
  // Flink DataStream
  //////////////////////////////////////////////////////////////////////////////////////
  /**
    * Generate the heatMaps for the input DenseVectors.
    *
    * @param streamOfLocalHeatMaps stream of locally calculated heatmaps.
    * @param globalHeatMapMethod   Method to use to generate global heatMap. (i.e. global-min-max").
    * @return A stream of heatmap values calculated on each mini-batches.
    */
  private def generateGlobalHeatMaps[GlobalW <: Window](streamOfLocalHeatMaps: DataStream[HeatMapValues],
                                                        globalWindowFunction: DataStream[IDElement[HeatMapValues]]
                                                          => AllWindowedStream[IDElement[HeatMapValues], GlobalW],
                                                        globalHeatMapMethod: HeatMapMethod.HeatMapTypes)
  : DataStream[HeatMapValues] = {
    require(HeatMapMethod.contains(globalHeatMapMethod), HeatMapMethod.toString)

    //    acquiring parameter generator
    val globalParamGenerator: AbstractRichFunction =
      HeatMapMethod
        .getGlobalParamGenerator(globalHeatMapMethod = globalHeatMapMethod)
        .get

    //    acquiring global heatMap generator
    val globalHeatMapFunction: AbstractRichFunction =
      HeatMapMethod
        .getGlobalHeatMapCalculator(globalHeatMapMethod = globalHeatMapMethod)
        .get

    val idedLocalHeatMaps: DataStream[IDElement[HeatMapValues]] =
      streamOfLocalHeatMaps
        .countWindowAll(1)
        .apply(new PrependElementID[HeatMapValues])

    val idedGlobalParams: DataStream[IDElement[GlobalParams]] =
      globalWindowFunction(idedLocalHeatMaps)
        .apply(globalParamGenerator
          .asInstanceOf[RichAllWindowFunction[IDElement[HeatMapValues], IDElement[GlobalParams], GlobalW]])

    val localHeatMapAndGlobalParams = idedLocalHeatMaps
      .join(idedGlobalParams)
      .where(_.elementID)
      .equalTo(_.elementID)
      .window(GlobalWindows.create())
      .trigger(CountTrigger.of(1))
      .apply(new StreamsCombiner[HeatMapValues, GlobalParams])


    val heatMapValues: DataStream[HeatMapValues] =
      localHeatMapAndGlobalParams
        .map(globalHeatMapFunction
          .asInstanceOf[RichMapFunction[(HeatMapValues, GlobalParams), HeatMapValues]])

    heatMapValues
  }

  /**
    * Generate the heatMaps for the input iterable of NamedVector.
    *
    * @param streamOfVectors     stream of input vectors.
    * @param localWindowFunction Window function to use for creating mini-batchs of given stream of input vectors
    * @param localHeatMapMethod  Method to use to generate heatMap. (i.e. "local-by-mean").
    * @return A stream of heatmap values calculated on each mini-batches.
    */
  private def generateLocalHeatMaps[LocalW <: Window](streamOfVectors: DataStream[NamedVector],
                                                      localWindowFunction: KeyedStream[SubtaskElement[NamedVector], Int]
                                                        => WindowedStream[SubtaskElement[NamedVector], Int, LocalW],
                                                      localHeatMapMethod: HeatMapMethod.HeatMapTypes)
  : DataStream[HeatMapValues] = {
    require(HeatMapMethod.contains(localHeatMapMethod), HeatMapMethod.toString)

    //    acquiring heat-map generator
    val localHeatMapCalculator: AbstractRichFunction =
      HeatMapMethod
        .getLocalHeatMapCalculator(localHeatMapMethod = localHeatMapMethod)
        .get

    // creating mini batch according to window policy provided
    val localWindowBatch: DataStream[Iterable[NamedVector]] = SubtaskWindowBatch.batch(inputStream = streamOfVectors, windowFunction = localWindowFunction)

    // calculate heatMap by using flatMap functionality on iterable of denseVectors.
    val heatMapValues: DataStream[HeatMapValues] =
      localWindowBatch
        .flatMap(localHeatMapCalculator
          .asInstanceOf[RichFlatMapFunction[Iterable[NamedVector], HeatMapValues]])

    heatMapValues
  }

  def generateHeatMapsFromWindows[LocalW <: Window, GlobalW <: Window](stream: DataStream[NamedVector],
                                                                       localWindowFunction: KeyedStream[SubtaskElement[NamedVector], Int]
                                                                         => WindowedStream[SubtaskElement[NamedVector], Int, LocalW],
                                                                       localHeatMapMethod: HeatMapMethod.HeatMapTypes,
                                                                       doubleWindowing: Boolean,
                                                                       globalWindowFunction: Option[DataStream[IDElement[HeatMapValues]] => AllWindowedStream[IDElement[HeatMapValues], GlobalW]],
                                                                       globalHeatMapMethod: Option[HeatMapMethod.HeatMapTypes])
  : DataStream[HeatMapValues] = {

    // calculate local heatMap values for given stream using associated heatMapMethod & window function
    val localHeatMapValues: DataStream[HeatMapValues] = generateLocalHeatMaps(
      streamOfVectors = stream,
      localWindowFunction = localWindowFunction,
      localHeatMapMethod = localHeatMapMethod
    )

    val heatMapValues =
      if (doubleWindowing) {
        require(globalHeatMapMethod.isDefined, "For double windowing, global heatMap methodology must be defined")
        require(globalWindowFunction.isDefined, "For double windowing, global windowing function must be defined")

        // calculate global heatMap values for given stream using associated heatMapMethod & window function, iff doubleWindowing is enabled
        val globalHeatMapValues: DataStream[HeatMapValues] = generateGlobalHeatMaps(
          streamOfLocalHeatMaps = localHeatMapValues,
          globalWindowFunction = globalWindowFunction.get,
          globalHeatMapMethod = globalHeatMapMethod.get
        )

        globalHeatMapValues
      }
      else {
        localHeatMapValues
      }

    heatMapValues.map(new AccumulatorUpdater)
  }

  private def getLocalWindowFunction(localWindowSize: Long)
  : KeyedStream[SubtaskElement[NamedVector], Int] => WindowedStream[SubtaskElement[NamedVector], Int, GlobalWindow] = {
    require(localWindowSize > 0, "Local window size must be positive number")
    (x: KeyedStream[SubtaskElement[NamedVector], Int]) => x.countWindow(localWindowSize)
  }

  private def getLocalWindowFunction(localWindowSize: Time)
  : KeyedStream[SubtaskElement[NamedVector], Int] => WindowedStream[SubtaskElement[NamedVector], Int, TimeWindow] = {
    (x: KeyedStream[SubtaskElement[NamedVector], Int]) => x.timeWindow(localWindowSize)
  }

  private def getGlobalWindowFunction(doubleWindowing: Boolean, globalWindowSize: Option[Long])
  : Option[DataStream[IDElement[HeatMapValues]] => AllWindowedStream[IDElement[HeatMapValues], GlobalWindow]] = {
    if (doubleWindowing) {
      // always, we will have sliding of 1 in global windowing
      val slideSize = 1L
      require(globalWindowSize.isDefined, "For double windowing, global window size must be defined")

      require(globalWindowSize.get > 0, "Global window size must be positive number")

      Some((x: DataStream[IDElement[HeatMapValues]]) => x.countWindowAll(size = globalWindowSize.get, slide = slideSize))
    }
    else {
      None
    }
  }

  /**
    * Compute the heatMap representation for the input DataStreams.
    *
    * @param stream              DataStream of NamedVector.
    * @param localHeatMapMethod  Method to use to generate heatMap. (i.e. "local-by-mean")
    * @param localWindowSize     Window size require to create mini-batch on which heatmap values would be generated
    * @param globalHeatMapMethod Method to use to generate heatMap on top of locally generated heatMap data
    * @param globalWindowSize    Window size for global windowing functionality (count window)
    * @return A stream of heatmap values
    */
  def createHeatMap(stream: DataStream[NamedVector],
                    localHeatMapMethod: HeatMapMethod.HeatMapTypes,
                    localWindowSize: Long,
                    globalHeatMapMethod: Option[HeatMapMethod.HeatMapTypes],
                    globalWindowSize: Option[Long],
                    doubleWindowing: Boolean)
  : DataStream[HeatMapValues] = {
    val localWindowFunction = getLocalWindowFunction(localWindowSize = localWindowSize)

    val globalWindowFunction = getGlobalWindowFunction(doubleWindowing = doubleWindowing, globalWindowSize = globalWindowSize)

    val heatMapValues: DataStream[HeatMapValues] = generateHeatMapsFromWindows(
      stream = stream,
      localWindowFunction = localWindowFunction,
      localHeatMapMethod = localHeatMapMethod,
      doubleWindowing = doubleWindowing,
      globalWindowFunction = globalWindowFunction,
      globalHeatMapMethod = globalHeatMapMethod
    )

    heatMapValues
  }

  /**
    * Compute the heatMap representation for the input DataStreams.
    *
    * @param stream              DataStream of NamedVector.
    * @param localHeatMapMethod  Method to use to generate heatMap. (i.e. "local-by-mean")
    * @param localWindowSize     Window time size require to create mini-batch on which heatmap values would be generated. (time window)
    * @param globalHeatMapMethod Method to use to generate heatMap on top of locally generated heatMap data
    * @param globalWindowSize    Window size for global windowing functionality (count window)
    * @return A stream of heatmap values
    */
  def createHeatMap(stream: DataStream[NamedVector],
                    localHeatMapMethod: HeatMapMethod.HeatMapTypes,
                    localWindowSize: Time,
                    globalHeatMapMethod: Option[HeatMapMethod.HeatMapTypes],
                    globalWindowSize: Option[Long],
                    doubleWindowing: Boolean)
  : DataStream[HeatMapValues] = {
    val localWindowFunction = getLocalWindowFunction(localWindowSize = localWindowSize)

    val globalWindowFunction = getGlobalWindowFunction(doubleWindowing = doubleWindowing, globalWindowSize = globalWindowSize)

    val heatMapValues: DataStream[HeatMapValues] = generateHeatMapsFromWindows(
      stream = stream,
      localWindowFunction = localWindowFunction,
      localHeatMapMethod = localHeatMapMethod,
      doubleWindowing = doubleWindowing,
      globalWindowFunction = globalWindowFunction,
      globalHeatMapMethod = globalHeatMapMethod
    )

    heatMapValues
  }
}

/**
  * Object maintains different supported HeatMap calculation methods
  *
  * After new methodologies are added, please update the documentation here.
  */
object HeatMapMethod extends Enumeration {
  type HeatMapTypes = Value
  val LocalByNormMean: Value = Value("local-by-norm-mean")
  val LocalByMean: Value = Value("local-by-mean")

  val GlobalByMinMaxScale: Value = Value("global-by-min-max-scale")
  val GlobalByStandardScale: Value = Value("global-by-standard-scale")

  def contains(heatMapMethod: Value): Boolean = values.exists(_.toString == heatMapMethod.toString)

  override def toString: String = s"Supported HeatMap Methods: ${values.mkString(", ")}"

  /**
    * Method directs proper local heatMap generator function based on method required
    *
    * @param localHeatMapMethod Method required.
    * @return A RichFunction associated with method.
    */
  def getLocalHeatMapCalculator(localHeatMapMethod: HeatMapTypes): Option[AbstractRichFunction] = {
    localHeatMapMethod match {
      case LocalByNormMean =>
        Some(new NormalizedMeanHeatMap)

      case LocalByMean =>
        Some(new MeanHeatMap)

      case _ =>
        None
    }
  }

  /**
    * Method directs proper global parameters generator function based on method required
    *
    * @param globalHeatMapMethod Method required.
    * @return A RichFunction associated with method.
    */
  def getGlobalParamGenerator(globalHeatMapMethod: HeatMapTypes): Option[AbstractRichFunction] = {
    globalHeatMapMethod match {
      case GlobalByMinMaxScale =>
        Some(new MinMaxParamsGenerator)

      case GlobalByStandardScale =>
        Some(new StandardScaleParamsGenerator)

      case _ =>
        None
    }
  }

  /**
    * Method directs proper global heatMap generator function based on method required
    *
    * @param globalHeatMapMethod Method required.
    * @return A RichFunction associated with method.
    */
  def getGlobalHeatMapCalculator(globalHeatMapMethod: HeatMapTypes): Option[AbstractRichFunction] = {
    globalHeatMapMethod match {
      case GlobalByMinMaxScale =>
        Some(new MinMaxHeatMap)

      case GlobalByStandardScale =>
        Some(new StandardScaleHeatMap)

      case _ =>
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
