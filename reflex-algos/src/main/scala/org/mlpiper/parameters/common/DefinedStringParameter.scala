package org.mlpiper.parameters.common


trait DefinedStringParameter extends StringParameter with DefinedParameter[String] {

  override val defaultValue: Option[String] = None

  override def condition(string: Option[String],
                         parameters: ArgumentParameterChecker)
  : Boolean = {
    super.condition(string, parameters) && string.get != null
  }
}
