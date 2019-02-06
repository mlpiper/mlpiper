package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, InputColComponentAttribute, OutputColComponentAttribute}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.Bucketizer

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class BucketizerComponent extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Bucketizer"
  override val description: String = "Transforms a column of continuous features to a column of bucketed features."
  override val version: String = "1.0.0"

  private val inOut = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)
  var splitsValues = Array[Double]()

  val bucketizer = new Bucketizer()

  val inputCol = InputColComponentAttribute()
  val outputCol = OutputColComponentAttribute()
  val splits = ComponentAttribute("splits", List[String](), "Splits", "Split points for mapping" +
    " continuous features into buckets. With n+1 splits, there are n buckets. A bucket defined by" +
    " splits x,y holds values in the range [x,y) except the last bucket, which also includes y." +
    " The splits should be of length >= 3 and strictly increasing. Values at -inf, inf must be" +
    " explicitly provided to cover all Double values otherwise, values outside the splits specified" +
    " will be treated as errors.")

  val handleInvalid = ComponentAttribute("handleInvalid", "error", "handleInvalid", "invalid data" +
    " (unseen labels or NULL values). Options are 'skip' (filter out rows with invalid data), " +
    "'error' (throw an error. Default), or 'keep' (put invalid data in a special additional bucket, " +
    "at index numLabels).", optional = true)
  handleInvalid.setOptions(List[(String, String)](("error", "error")
    , ("skip", "skip")
    , ("keep", "keep")
  ))


  attrPack.add(inputCol, outputCol, handleInvalid, splits)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    if (paramMap.contains(handleInvalid.key)) {
      bucketizer.setHandleInvalid(handleInvalid.value)
    }
    splitsValues = paramMap(splits.key).asInstanceOf[List[String]]
      .map(_.toString.trim().toDouble).toArray
  }

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val inputColName = inputCol.value
    val outputColName = outputCol.value

    bucketizer
      .setInputCol(inputColName)
      .setOutputCol(outputColName)
      .setSplits(splitsValues)

    pipelineInfo.addStage(
      stage = bucketizer,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
