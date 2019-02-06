package org.apache.flink.streaming.scala.examples.common.parameters.feature

import org.apache.flink.streaming.scala.examples.common.parameters.common.PositiveIntParameter

case object WindowSize extends PositiveIntParameter {
  override val key: String = "windowSize"
  override val required: Boolean = true
  override val description: String = " # of samples to batch (i.e., micro batch size). (Default: 10000)"
  override val label: String = " # of samples to batch (i.e., micro batch size)"
  override val defaultValue: Option[Int] = Some(10000)
}