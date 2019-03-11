package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, InputColComponentAttribute, OutputColComponentAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorIndexer
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class VectorIndexerComponent extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "VectorIndexer"
  override val description: String = "Indexes categorical features in a vector."
  override val version: String = "1.0.0"

  private val inOut = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)

  val vectorIndexer = new VectorIndexer()
  val inputCol = InputColComponentAttribute()
  val outputCol = OutputColComponentAttribute()

  val maxCategories = ComponentAttribute("maxCategories", 20, "Maximum Categories", "Threshold for the" +
    "number of values a categorical feature can take (>= 2). If a feature is found to " +
    "have > maxCategories values, then it is declared continuous. (Default: 20)", optional = true)
    .setValidator(x => x >= 2)

  attrPack.add(inputCol, outputCol, maxCategories)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    if (paramMap.contains(maxCategories.key)) {
      vectorIndexer.setMaxCategories(maxCategories.value)
    }
  }

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val inputColName = inputCol.value
    val outputColName = outputCol.value

    vectorIndexer
      .setInputCol(inputColName)
      .setOutputCol(outputColName)

    pipelineInfo.addStage(
      stage = vectorIndexer,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
