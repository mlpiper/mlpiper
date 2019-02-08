package org.mlpiper.parameters.parsing

import com.parallelmachines.reflex.pipeline.JsonHeaders
import org.mlpiper.parameters.common.{ArgumentParameterChecker, ArgumentParameterTool, IntParameter, StringParameter}
import org.mlpiper.parameters.ml.Validation
import org.mlpiper.parameters.performance.PerformanceMetrics
import org.mlpiper.utils.ParameterIndices
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap

sealed trait Separator extends StringParameter {
  protected val validSeparators: String = Separator.validSeparators

  override val defaultValue: Option[String] = None
  override val required = false
  override val description = validSeparators + " Separator of elements."
  override lazy val errorMessage: String = "Not a valid " + key +
    ". Must be one of the following: " + Separator.validSeparators

  override def condition(value: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    value.isDefined && Separator.contains(value.get)
  }

  override def toJson(): String = {
    var res = super.toJson
    res = res.dropRight(1) + ", "
    res += jsonPair(s"${JsonHeaders.UITypeHeader}", s""""select"""") + ", "
    res +=
      s""""options" : [{"${JsonHeaders.LabelHeader}":"Comma (,)", "value":"comma"},
         |             {"${JsonHeaders.LabelHeader}":"Semicolon (;)", "value":"semicolon"},
         |             {"${JsonHeaders.LabelHeader}":"Space ( )", "value":"space"},
         |             {"${JsonHeaders.LabelHeader}":"Backtick (`)", "value":"backtick"}]""".stripMargin
    res += "}"
    res
  }
}

case object DataSeparator extends Separator {
  override val key: String = "dataSeparator"
  override val label: String = "Data Separator"
  override val defaultValue: Option[String] = Some("comma")
  override val description = s"Vector elements separator. $validSeparators (Default: comma)"
}

case object LabelSeparator extends Separator {
  override val key = "labeledDataSeparator"
  override val label: String = "Labeled Data Separator"
  override val defaultValue: Option[String] = None
  override val description = s"Char that separates a label prepended to a vector, $validSeparators"
}

case object TimestampSeparator extends Separator {
  override val key = "timestampSeparator"
  override val label: String = "Timestamp Separator"
  override val defaultValue: Option[String] = None
  override val description = s"Char that separates a timestamp appended to a vector, $validSeparators"
}

case object LabelIndex extends IntParameter {
  override val key = "labelIndex"
  override val label: String = "Label Index"
  override val defaultValue: Option[Int] = None
  override val required = false
  override val description = "Index(0 based) of the value to be used as a label"
  override lazy val errorMessage: String = key + " must be greater than zero"

  override def condition(x: Option[Int], parameters: ArgumentParameterChecker)
  : Boolean = {
    x.isDefined && x.get >= 0
  }
}

case object TimestampIndex extends IntParameter {
  override val key = "timestampIndex"
  override val label: String = "Timestamp Index"
  override val defaultValue: Option[Int] = None
  override val required = false
  override val description = "Index(0 based) of the value to be used as a timestamp"
  override lazy val errorMessage: String = key + " must be greater than zero"

  override def condition(x: Option[Int], parameters: ArgumentParameterChecker)
  : Boolean = {
    x.isDefined && x.get >= 0
  }
}

case object IndicesRange extends StringParameter {
  override val key = "indicesRange"
  override val label: String = "Indices Range"
  override val defaultValue: Option[String] = None
  override val required = false
  override val description = "Indices to parse. If omitted, all attributes will be parsed. Example: 0,3,5-8"
  override lazy val errorMessage: String = key + " must be not empty"

  override def condition(value: Option[String],
                         parameters: ArgumentParameterChecker): Boolean = {
    value.isDefined && ParameterIndices.validParameterIndices(value.get)
  }
}

object Separator {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  private val separatorMap = HashMap[String, Char](
    "comma" -> ',',
    "semicolon" -> ';',
    "space" -> ' ',
    "backtick" -> '`'
  )

  private[Separator] def validSeparators: String = {
    '[' + separatorMap.keys.mkString("/") + ']'
  }

  def contains(name: String): Boolean = {
    separatorMap.contains(name)
  }

  def apply(name: String): Char = {
    require(separatorMap.contains(name), "Invalid separator name '" + name + '\'')
    separatorMap(name)
  }

  /**
    * @param params [[ArgumentParameterTool]]
    * @return If [[PerformanceMetrics]] is true, it attempts to retrieve [[TimestampSeparator]].
    *         Otherwise it returns None.
    */
  def getTimestampSeparatorForPerformanceMetrics(params: ArgumentParameterTool)
  : Option[Char] = {
    if (params.getBoolean(PerformanceMetrics)) {
      if (params.containsUserSpecified(TimestampSeparator)) {
        Some(apply(params.getString(TimestampSeparator)))
      } else {
        None
      }
    } else {
      if (params.containsUserSpecified(TimestampSeparator)) {
        LOG.info("Ignoring " + TimestampSeparator.key + " since " + PerformanceMetrics.key +
          " is false")
      }
      None
    }
  }

  /**
    * @param params [[ArgumentParameterTool]]
    * @return If [[Validation]] is true, it attempts to retrieve [[LabelSeparator]].
    *         Otherwise it returns None.
    */
  def getLabelSeparatorForValidation(params: ArgumentParameterTool)
  : Option[Char] = {
    if (params.getBoolean(Validation)) {
      Some(Separator(params.getString(LabelSeparator)))
    } else {
      if (params.containsUserSpecified(LabelSeparator)) {
        LOG.info("Ignoring " + LabelSeparator.key + " since " + Validation.key + " is false")
      }
      None
    }
  }

}
