package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, InputColComponentAttribute, OutputColComponentAttribute}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.Binarizer

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class BinarizerComponent extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Binarizer"
  override val description: String = "Thresholding numerical features to binary (0/1) features. " +
    "Both vector and scalar features supported by Binarizer."
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
  val threshold = ComponentAttribute("threshold", 0.0, "Threshold", "The threshold for binarization" +
    "(Default: 0)")

  attrPack.add(inputCol, outputCol, threshold)

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val inputColName = inputCol.value
    val outputColName = outputCol.value
    val thresholdValue = threshold.value

    val binarizer = new Binarizer()
      .setInputCol(inputColName)
      .setOutputCol(outputColName)
      .setThreshold(thresholdValue)

    pipelineInfo.addStage(
      stage = binarizer,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
