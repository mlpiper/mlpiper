package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.components.flink.streaming.StreamExecutionEnvironment
import com.parallelmachines.reflex.pipeline.{CanaryConfig, DataWrapperBase}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * EventSocketTwoSource is a subclass of EventSocketSource.
  * Which produces exactly 2 outputs.
  *
  * Configured to work with labels provided in systemConfig.canaryConfig
  */
class EventSocketTwoSource extends EventSocketSource {

  override val label = "Event Socket Source for Comparator"
  override val description = "Receives events and separates them into two streams"
  override val version = "1.0.0"
  override lazy val isVisible = false

  var label1 = ""
  var label2 = ""

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    require(paramMap.contains("canaryConfig"), "canaryConfig section was not found in systemConfig")
    val cc = paramMap("canaryConfig").asInstanceOf[CanaryConfig]
    require(cc.canaryLabel1.isDefined && cc.canaryLabel2.isDefined, "canaryConfig section must contain two labels")
    label1 = cc.canaryLabel1.get
    label2 = cc.canaryLabel2.get
  }

  override def materialize(env: StreamExecutionEnvironment, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    ArrayBuffer[DataWrapperBase]()
  }
}