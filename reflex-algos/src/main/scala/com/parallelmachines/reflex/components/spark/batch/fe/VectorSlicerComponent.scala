package com.parallelmachines.reflex.components.spark.batch.fe

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, InputColComponentAttribute, OutputColComponentAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorSlicer
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class VectorSlicerComponent extends SparkBatchComponent {

  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.featureEng
  override val label: String = "Vector Slicer"
  override val description: String = "Slice from a vector a given list of indices and/or names."
  override val version: String = "1.0.0"

  var includeIndicesValues = Array[Int]()
  var includeAtrrNames = Array[String]()

  private val inOut = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(inOut)
  override var outputTypes: ConnectionList = ConnectionList(inOut)
  val vectorSlicer = new VectorSlicer()


  val includeIndices = ComponentAttribute("includeIndices", List[String](), "Included Indices",
    "Indices of columns that will be included into the sliced vector.", optional = true)
  //TODO: Check Attribute naming usage support
  val includeNames = ComponentAttribute("includeNames", List[String](), "included Names",
    "Names of attributes that will be included into the sliced vector. Indices will be handled" +
      " first if both provided. This requires the use of NumericAttribute naming which are not " +
      "provided for now.", optional = true)

  val inputColName = InputColComponentAttribute()
  val outputCol = OutputColComponentAttribute()

  attrPack.add(inputColName, includeIndices, includeNames, outputCol)

  override def configure(paramMap: Map[String, Any]): Unit = {
    super.configure(paramMap)
    // casting is required, because ComponentAttribute doesn't cast List types

    if (paramMap.contains(includeIndices.key)) {
      includeIndicesValues = includeIndices.value
        .map(_.toString.trim())
        .map(_.toInt).toArray
      vectorSlicer.setIndices(includeIndicesValues)
    }
    if (paramMap.contains(includeNames.key)) {
      includeAtrrNames = paramMap(includeNames.key).asInstanceOf[List[String]]
        .map(_.toString.trim()).toArray
      vectorSlicer.setNames(includeAtrrNames)
    }
    require((!includeIndicesValues.isEmpty) || (!includeAtrrNames.isEmpty),
      s" At least one attribute, ${includeIndices.key} or ${includeNames.key}, should be selected for ${getClass.getSimpleName}")

  }

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase],
                           errPrefixStr: String): ArrayBuffer[DataWrapperBase] = {

    val pipelineInfo = dsArr(0).data[SparkBatchPipelineInfo]()
    val outputColName = outputCol.value

    vectorSlicer
      .setInputCol(inputColName.value)
      .setOutputCol(outputColName)

    pipelineInfo.addStage(
      stage = vectorSlicer,
      addCols = Some(Array(outputColName))
    )

    ArrayBuffer(new DataWrapper(pipelineInfo))
  }
}
