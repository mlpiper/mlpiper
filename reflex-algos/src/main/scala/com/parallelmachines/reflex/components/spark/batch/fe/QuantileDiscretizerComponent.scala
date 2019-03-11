package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, InputColComponentAttribute, OutputColComponentAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class QuantileDiscretizerComponent extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "QuantileDiscretizer"
  override val description: String = "Transforms a column of continuous features to a " +
    "column of binned categorical features."
  override val version: String = "1.0.0"

  private val inOut = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)

  val quantil = new QuantileDiscretizer()


  val inputCol = InputColComponentAttribute()
  val outputCol = OutputColComponentAttribute()
  val numBuckets = ComponentAttribute("numBuckets", 2, "Number of Buckets", "Number of buckets (quantiles," +
    " or categories) into which data points are grouped. Must be >= 2. (Default: 2).", optional = true)
    .setValidator(x => x >= 2)
  val relativeError = ComponentAttribute("relativeError", 0.001, "Relative Error", "The relative target" +
    " precision for the approximate quantile algorithm used to generate buckets. " +
    "Must be in the range [0.0, 1.0]. (Default: 0.001)", optional = true)
    .setValidator(x => (x >= 0.0) & (x <= 1.0))
  val handleInvalid = ComponentAttribute("handleInvalid", "error", "handleInvalid", "invalid data" +
    " (unseen labels or NULL values). Options are 'skip' (filter out rows with invalid data), " +
    "'error' (throw an error. Default), or 'keep' (put invalid data in a special additional bucket, " +
    "at index numLabels).", optional = true)
  handleInvalid.setOptions(List[(String, String)](("error", "error")
    , ("skip", "skip")
    , ("keep", "keep")
  ))


  attrPack.add(inputCol, outputCol, handleInvalid, numBuckets, relativeError)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    if (paramMap.contains(handleInvalid.key)) {
      quantil.setHandleInvalid(handleInvalid.value)
    }
    if (paramMap.contains(numBuckets.key)) {
      quantil.setNumBuckets(numBuckets.value)
    }
    if (paramMap.contains(relativeError.key)) {
      quantil.setRelativeError(relativeError.value)
    }
  }

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val inputColName = inputCol.value
    val outputColName = outputCol.value

    quantil
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    pipelineInfo.addStage(
      stage = quantil,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
