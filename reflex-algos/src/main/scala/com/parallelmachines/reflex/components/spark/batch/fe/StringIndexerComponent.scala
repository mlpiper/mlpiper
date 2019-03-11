package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, InputColComponentAttribute, OutputColComponentAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StringIndexer
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class StringIndexerComponent extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "StringIndexer"
  override val description: String = "Encodes a string column of labels to a column of label " +
    "indices. The indices are in [0, numLabels), ordered by label frequencies."
  override val version: String = "1.0.0"

  private val inOut = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)

  val stringIndexer = new StringIndexer()
  val inputCol = InputColComponentAttribute()
  val outputCol = OutputColComponentAttribute()
  val handleInvalid = ComponentAttribute("handleInvalid", "error", "handleInvalid", "invalid data" +
    " (unseen labels or NULL values). Options are 'skip' (filter out rows with invalid data), " +
    "'error' (throw an error. Default), or 'keep' (put invalid data in a special additional bucket, " +
    "at index numLabels).", optional = true)
  handleInvalid.setOptions(List[(String, String)](("error", "error")
    //TODO: Add according to version supports
    //    , ("skip", "skip")
    //    , ("keep", "keep")
  ))

  attrPack.add(inputCol, outputCol, handleInvalid)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    if (paramMap.contains(handleInvalid.key)) {
      stringIndexer.setHandleInvalid(handleInvalid.value)
    }
  }

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val inputColName = inputCol.value
    val outputColName = outputCol.value

    stringIndexer
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    pipelineInfo.addStage(
      stage = stringIndexer,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
