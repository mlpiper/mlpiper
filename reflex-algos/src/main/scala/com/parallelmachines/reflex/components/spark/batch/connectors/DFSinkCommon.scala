package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import org.apache.spark.sql.DataFrame

trait DFSinkCommon extends SparkBatchComponent {
  var includeInputColsNames = Array[String]()
  var excludeInputColsNames = Array[String]()
  val includeInputCols = ComponentAttribute("includeCols", List[String](), "Include columns",
    "Names of columns that will be included into resulting Dataframe. (Default: all columns)", optional = true)
  val excludeInputCols = ComponentAttribute("excludeCols", List[String](), "Exclude columns",
    "Names of columns that will not be included into resulting Dataframe. (Default: none)", optional = true)

  attrPack.add(includeInputCols, excludeInputCols)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    // casting is required, because ComponentAttribute doesn't cast List types
    if (paramMap.contains(includeInputCols.key)) {
      includeInputColsNames = paramMap(includeInputCols.key).asInstanceOf[List[String]]
        .map(_.toString.trim()).toArray
    }
    if (paramMap.contains(excludeInputCols.key)) {
      excludeInputColsNames = paramMap(excludeInputCols.key).asInstanceOf[List[String]]
        .map(_.toString.trim()).toArray
    }

    // we fail when both include and exclude are provided - only one of the two is legal
    // i.e. fail when x.isNonEmpty and y.isNonEmpty
    // or require(!(x.isNonEmpty && y.isNonEmpty))
    // or require(x.is Empty || y.isEmpty)
    require(includeInputColsNames.isEmpty || excludeInputColsNames.isEmpty,
      s"Only one parameter ${includeInputCols.key} or ${excludeInputCols.key} can be used in ${getClass.getSimpleName}")
  }

  def colsToDrop(df: DataFrame): Array[String] = {
    var inputColNames = df.columns
    // selecting cols name directly if inclusion list is given
    if (!includeInputColsNames.isEmpty) {
      inputColNames = includeInputColsNames
    }

    // filtering out cols name directly if exclusion list is given
    if (!excludeInputColsNames.isEmpty) {
      inputColNames = inputColNames
        .filter(item => !excludeInputColsNames.contains(item))
    }
    df.columns.filter(x => !inputColNames.contains(x))
  }
}
