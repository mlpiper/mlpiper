package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, InputColComponentAttribute, OutputColComponentAttribute}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.PCA

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class PCAFE extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "PCA"
  override val description: String = "PCA with k elements."
  override val version: String = "1.0.0"

  private val inOut = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)

  val inputCol = InputColComponentAttribute()
  val outputCol = OutputColComponentAttribute()
  val k = ComponentAttribute("k", 0, "Number of Principal Components", "the number of principal" +
    " components (> 0). (Default: 0)").setValidator(x => x >= 0)

  attrPack.add(inputCol, outputCol, k)

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val inputColName = inputCol.value
    val outputColName = outputCol.value
    val kValue = k.value

    val pca = new PCA()
      .setInputCol(inputColName)
      .setOutputCol(outputColName)
      .setK(kValue)

    pipelineInfo.addStage(
      stage = pca,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
