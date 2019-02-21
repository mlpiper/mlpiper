package com.parallelmachines.reflex.common

import org.apache.flink.streaming.scala.examples.common.stats.{AccumulatorInfoJsonHeaders, AccumulatorJsonHeaders}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.slf4j.LoggerFactory

import scala.collection.{immutable, mutable}

object HealthType extends Enumeration {
  type HealthType = Value

  val ContinuousHistogramHealth: HealthType.Value = Value("continuousDataHistogram")
  val CategoricalHistogramHealth: HealthType.Value = Value("categoricalDataHistogram")
  val PredictionHistogramHealth: HealthType.Value = Value("PredictionHistogram")
}

/**
  * Class to parse Health Events sent by ECO.
  * Should be moved to Protobuf */
class HealthWrapper(var accumName: String, var data: String) {

  def nameEqualsOrContains(pattern: String): Boolean = {
    this.accumName == pattern || this.accumName.startsWith(s"$pattern.")
  }
}

object HealthWrapper {

  private val logger = LoggerFactory.getLogger(classOf[HealthWrapper])

  def apply(accumName: String, data: String): HealthWrapper = new HealthWrapper(accumName, data)

  def apply(accumInfoJson: String): HealthWrapper = {
    var accumName: String = ""
    var accumData: String = ""

    try {
      val accumInfoJsonMap = Json(DefaultFormats).parse(accumInfoJson).values.asInstanceOf[immutable.Map[String, Any]]
      accumData = accumInfoJsonMap(AccumulatorInfoJsonHeaders.DataKey).asInstanceOf[String]
      accumName = accumInfoJsonMap(AccumulatorInfoJsonHeaders.NameKey).asInstanceOf[String]
    } catch {
      case _: Throwable =>
        logger.error(s"Failed to parse AccumulatorInfo JSON: $accumInfoJson")
        accumName = ""
        accumData = ""
    }
    new HealthWrapper(accumName = accumName, data = accumData)
  }
}

/** Base class for HealthLib.
  * */
trait HealthLib extends Serializable {

  val enabled: Boolean

  /** List of registered components */
  protected var registeredComponents = mutable.ListMap[String, HealthComponent]()

  /** When incoming health is provided, flag must be set to true.
    * It shows that comparison must be performed.  */
  protected var hasIncomingHealth: Boolean = _

  /** Should be overriden by HealthLib implementation for each engine.
    * Is used to multiplex incoming Health messages from training among health components. */
  protected def init(): Unit

  /**
    * Main routine to generate health data.
    *
    * If incoming health data is provided, HealthComponent.generateHealthAndCompare() will be called
    **/
  final def generateHealth(): Unit = {
    if (!enabled) return
    require(registeredComponents.nonEmpty, "HealthLib components list is empty, probably this is not what you want")

    init()
    if (!hasIncomingHealth) {
      registeredComponents.foreach(x => x._2.generateHealth())
    } else {
      registeredComponents.foreach(x => x._2.generateHealthAndCompare())
    }
  }

  /**
    * Register instance of HealthComponent in HealthLib.
    **/
  def addComponent(comp: HealthComponent): HealthLib = {
    val key = comp.getName()
    require(!registeredComponents.contains(key), s"HealthLib already contains component registered for $key key")
    this.registeredComponents(key) = comp
    this
  }
}
