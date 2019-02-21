package com.parallelmachines.reflex.common

import scala.collection.mutable

/**
  * Object [[HealthConfigurations]] is responsible for providing defaults for healths
  */
object HealthConfigurations extends Serializable {
  private val healthConfMap: mutable.Map[HealthConfEnum.Type, Any] = mutable.Map[HealthConfEnum.Type, Any](
    HealthConfEnum.NumberPrecision -> 4,
    HealthConfEnum.MaxCategoryInHistogram -> 13,
    HealthConfEnum.EnableNumberPrecision -> false
  )

  def getConf(confType: HealthConfEnum.Type): Any = {
    healthConfMap(confType)
  }
}

object HealthConfEnum extends Enumeration {
  type Type = Value

  val EnableNumberPrecision: HealthConfEnum.Type = Value("EnableNumberPrecision")
  val NumberPrecision: HealthConfEnum.Type = Value("NumberPrecision")
  val MaxCategoryInHistogram: HealthConfEnum.Type = Value("MaxCategoryInHistogram")
}
