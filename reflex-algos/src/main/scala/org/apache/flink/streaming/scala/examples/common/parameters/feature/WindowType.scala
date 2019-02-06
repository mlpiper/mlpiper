package org.apache.flink.streaming.scala.examples.common.parameters.feature

import com.parallelmachines.reflex.pipeline.JsonHeaders
import org.apache.flink.streaming.scala.examples.common.parameters.common.StringParameter
import org.apache.flink.streaming.scala.examples.common.parameters.tools.ArgumentParameterChecker

case object WindowType extends StringParameter {
  override val key: String = "windowType"
  override val required: Boolean = true
  override val description: String = "Windowing Type [Count/Time]"
  override val errorMessage: String = "Windowing Not Supported"
  override val label: String = "Windowing Type"
  override val defaultValue: Option[String] = None

  override def condition(value: Option[String], parameters: ArgumentParameterChecker): Boolean = value.isDefined

  override def toJson(): String = {
    var res = super.toJson()
    res = res.dropRight(1) + ", "
    res += jsonPair(s"${JsonHeaders.UITypeHeader}", s""""select"""") + ", "
    res += s""""options" : [{"${JsonHeaders.LabelHeader}":"Count", "value":"count"}, {"${JsonHeaders.LabelHeader}":"Time", "value":"time"}]"""
    res += "}"
    res
  }
}