package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, InputColComponentAttribute, OutputColComponentAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.Normalizer
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class NormalizerPipeline extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Normalizer"
  override val description: String = "Transforms a column of vectors into a normalized column of vectors."
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
  val pNorm = ComponentAttribute("pNorm", 1.0, "PNorm", "p-norm order. (Default: 1.0)." +
    "p-norm is the pth root of the sum of the pth-powers of the vector elements.", optional = true)

  attrPack.add(inputCol, outputCol, pNorm)

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val inputColName = inputCol.value
    val outputColName = outputCol.value
    val pNormValue = pNorm.value

    val normalizer = new Normalizer()
      .setInputCol(inputColName)
      .setOutputCol(outputColName)
      .setP(pNormValue)

    pipelineInfo.addStage(
      stage = normalizer,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
