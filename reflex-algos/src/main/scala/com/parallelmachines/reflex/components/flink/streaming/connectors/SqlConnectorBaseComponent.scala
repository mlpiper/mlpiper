package com.parallelmachines.reflex.components.flink.streaming.connectors

import com.parallelmachines.reflex.components.ComponentAttribute.ComponentAttribute

trait SqlConnectorBaseComponent {
  val sqlUrl: ComponentAttribute[String]
  val sqlUsername: ComponentAttribute[String]
  val sqlPassword: ComponentAttribute[String]
  val labelName: ComponentAttribute[String]
}
