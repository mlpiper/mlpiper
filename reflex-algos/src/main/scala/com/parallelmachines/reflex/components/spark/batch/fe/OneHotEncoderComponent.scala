package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, InputColComponentAttribute, OutputColComponentAttribute}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.OneHotEncoder

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class OneHotEncoderComponent extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "OneHotEncoder"
  override val description: String = "Maps a column of label indices to a column of binary vectors," +
    " with at most a single one-value."
  override val version: String = "1.0.0"

  private val inOut = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)
  val oneHot = new OneHotEncoder()


  val inputCol = InputColComponentAttribute()
  val outputCol = OutputColComponentAttribute()

  val dropLast = ComponentAttribute("dropLast", true, "Drop Last", "Whether to drop the " +
    "last category. (Default: true)", optional = true)

  attrPack.add(inputCol, outputCol, dropLast)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    if (paramMap.contains(dropLast.key)) {
      oneHot.setDropLast(dropLast.value)
    }
  }

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val inputColName = inputCol.value
    val outputColName = outputCol.value

    oneHot
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    pipelineInfo.addStage(
      stage = oneHot,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
