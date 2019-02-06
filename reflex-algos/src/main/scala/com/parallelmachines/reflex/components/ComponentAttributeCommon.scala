package com.parallelmachines.reflex.components

import com.parallelmachines.reflex.components.ComponentAttribute.ComponentAttribute

object LabelColComponentAttribute {
  def apply(optional: Boolean = true) = ComponentAttribute("labelCol", "label", "Label Column",
    "Labels column name", optional = optional)
}

object FeaturesColComponentAttribute {
  def apply() = ComponentAttribute("featuresCol", "features", "Features Column",
    "Features column name", optional = true)
}

object InputColComponentAttribute {
  def apply() = ComponentAttribute("inputCol", "", "Input Column", "Input column name")
}

object OutputColComponentAttribute {
  def apply() = ComponentAttribute("outputCol", "", "Output Column", "Output column name")
}

object EnableValidationComponentAttribute {
  def apply() = ComponentAttribute("enableValidation", false, "Enable Validation", "Allow for validation",
    optional = true)
}

object EnablePerformanceComponentAttribute {
  def apply() = ComponentAttribute("enablePerformance", false, "Enable Performance Measurements",
    "Switch to enable collection of performance metrics", optional = true)
}

object SeparatorComponentAttribute {
  def apply(): ComponentAttribute[String] = {
    val separator = ComponentAttribute("separator", ",", "Separator", "Column separator. (Default: comma (,))", optional = true)
    separator.setOptions(List[(String, String)](("comma (,)", ","), ("semicolon (;)", ";"), ("space ( )", " ")))
    separator
  }
}

object PredictionColComponentAttribute {
  def apply() = ComponentAttribute("predictionCol", "prediction", "Prediction Column",
    "Prediction column name", optional = true)
}

object SQLHostNameComponentAttribute {
  def apply() = ComponentAttribute("sqlHostName", "localhost", "Host Name", "SQL Host")
}

object SQLHostPortComponentAttribute {
  def apply() = ComponentAttribute("sqlPort", 3306, "Port", "SQL Port")
}

object SQLDataBaseNameComponentAttribute {
  def apply() = ComponentAttribute("sqlDatabase", "db", "Database", "DB To Read From")
}

object SQLTableNameComponentAttribute {
  def apply() = ComponentAttribute("sqlTable", "sql_table", "Table Name", "Table Name")
}

object SQLQueryComponentAttribute {
  def apply() = ComponentAttribute("sqlQuery", "select * from table", "Query", "Query")
}

object SQLUserNameComponentAttribute {
  def apply() = ComponentAttribute("sqlUsername", "admin", "UserName", "User Name")
}

object SQLPasswordComponentAttribute {
  def apply() = ComponentAttribute("sqlPassword", "admin", "Password", "Password")
}
