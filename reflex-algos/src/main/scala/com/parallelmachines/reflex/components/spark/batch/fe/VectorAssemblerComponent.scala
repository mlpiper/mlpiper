package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, OutputColComponentAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorAssembler
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class VectorAssemblerComponent extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Vector assembler"
  override val description: String = "Combines a given list of columns into a single vector column."
  override val version: String = "1.0.0"

  var includeInputColsNames = Array[String]()
  var excludeInputColsNames = Array[String]()

  private val inOut = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)

  val includeInputCols = ComponentAttribute("includeCols", List[String](), "Include columns",
    "Names of columns that will be included into assembled vector. If field is empty then picking" +
      " all the columns from the original dataframes and not from the previous stage. " +
      "Provide name of vector in 'outputCol' field.", optional = true)
  val excludeInputCols = ComponentAttribute("excludeCols", List[String](), "Exclude columns",
    "Names of columns that will not be included into assembled vector. Provide name of vector in" +
      " 'outputCol' field.", optional = true)
  val outputCol = OutputColComponentAttribute()

  attrPack.add(includeInputCols, excludeInputCols, outputCol)

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

    require(includeInputColsNames.isEmpty || excludeInputColsNames.isEmpty, s"Only one parameter ${includeInputCols.key} or ${excludeInputCols.key} can be used in ${getClass.getSimpleName}")
  }

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val outputColName = outputCol.value
    var inputColNames = Array[String]()

    inputColNames = pipelineInfo.getCurrentDataframeColumns

    // selecting cols name directly if inclusion list is given
    if (!includeInputColsNames.isEmpty) {
      inputColNames = includeInputColsNames
    }

    // filtering out cols name directly if exclusion list is given
    if (!excludeInputColsNames.isEmpty) {
      inputColNames = inputColNames
        .filter(item => !excludeInputColsNames.contains(item))
    }

    val vectorAssembler = new VectorAssembler()
      .setInputCols(inputColNames)
      .setOutputCol(outputColName)

    pipelineInfo.addStage(
      stage = vectorAssembler,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
