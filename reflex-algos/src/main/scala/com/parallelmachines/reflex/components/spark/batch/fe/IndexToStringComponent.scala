package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, InputColComponentAttribute, OutputColComponentAttribute}
import com.parallelmachines.reflex.pipeline.{ConnectionGroups, _}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.IndexToString

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class IndexToStringComponent extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "IndexToString"
  override val description: String = "Maps a column of label indices back to a column containing" +
    " the original labels as strings."
  override val version: String = "1.0.0"

  private val inOut = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)
  val indexToString = new IndexToString()

  val inputCol = InputColComponentAttribute()
  val outputCol = OutputColComponentAttribute()
  val labels = ComponentAttribute("labels", List[String](), "Labels", "Array of labels" +
    " specifying index-string mapping. If not provided or if empty, then metadata from inputCol" +
    " is used instead.")

  // Not optional. In spark ML the label field is optional, if not the labels are taken from the
  // DF schema of the column. However getlabel() fails and JPMML uses it when it tries to get
  // these labels.

  attrPack.add(inputCol, outputCol, labels)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    if (paramMap.contains(labels.key)) {
      indexToString.setLabels(paramMap(labels.key).asInstanceOf[List[String]]
        .map(_.toString.trim()).toArray)
    }
  }

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val inputColName = inputCol.value
    val outputColName = outputCol.value

    indexToString
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    pipelineInfo.addStage(
      stage = indexToString,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
