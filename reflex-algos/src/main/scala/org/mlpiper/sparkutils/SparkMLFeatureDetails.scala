package org.mlpiper.sparkutils

import com.parallelmachines.reflex.common.enums.OpType
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{PipelineModel, PredictionModel}
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * This Class fetches feature information form sparkML models and dataframe
  */
class SparkMLFeatureDetails {
  private var stageOutputNameList = new ArrayBuffer[String](0)
  private var stageInputNameList = new ArrayBuffer[ArrayBuffer[String]](0)
  private var stageInputTypeList = new ArrayBuffer[ArrayBuffer[OpType]](0)

  /**
    * Function is responsible for providing features category details.
    *
    * @param sparkMLModel : Pipeline Model
    * @param dfSchema     : DataFrame Schema
    */
  def getFeatureCategory(sparkMLModel: PipelineModel, dfSchema: StructType): Map[String, OpType] = {
    val pipelineLen = sparkMLModel.stages.length - 1
    val lastPipelineStage = sparkMLModel.stages(pipelineLen)
    var featuresColName = ""

    lastPipelineStage match {
      case i: KMeansModel =>
        val algoModel = i
        featuresColName = algoModel.getFeaturesCol

      case _ =>
        val algoModel = lastPipelineStage.asInstanceOf[PredictionModel[_, _]]
        featuresColName = algoModel.getFeaturesCol
    }

    val featuresDetails: Map[String, OpType] =
      parseFeatureNameType(sparkMLModel, pipelineLen, dfSchema, featuresColName)

    featuresDetails
  }


  /**
    * Function is adding features to the list for "regular" stages
    * It scans the inputs and get the origin features for each input list
    *
    * @param inputColsName  : Input Column name
    * @param outputColsName : Output Column name
    * @param dfSchema       : DataFrame schema
    * @param inputType      : input type if exists
    */
  def addFeatureToList(inputColsName: String, outputColsName: String, dfSchema: StructType, inputType: Option[OpType] = None): Unit = {
    var elementExists = false
    for (nameElementsIndex <- stageOutputNameList.indices) {
      if (stageOutputNameList(nameElementsIndex) == inputColsName) {
        elementExists = true
        stageInputNameList = stageInputNameList :+ stageInputNameList(nameElementsIndex)
        if (inputType.isEmpty) {
          stageInputTypeList = stageInputTypeList :+ stageInputTypeList(nameElementsIndex)
        }
        else {
          stageInputTypeList = stageInputTypeList :+ ArrayBuffer(inputType.get)
        }
      }
    }
    if (!elementExists) {
      if (inputType.isEmpty) {
        if (dfSchema.fields(dfSchema.fieldIndex(inputColsName)).dataType == StringType) {
          //TODO: Char, Binary, DateTime?
          stageInputTypeList = stageInputTypeList :+ ArrayBuffer(OpType.CATEGORICAL)
        }
        else {
          stageInputTypeList = stageInputTypeList :+ ArrayBuffer(OpType.CONTINUOUS)
        }
      }
      else {
        stageInputTypeList = stageInputTypeList :+ ArrayBuffer(inputType.get)
      }
      stageInputNameList = stageInputNameList :+ ArrayBuffer(inputColsName)
    }
    stageOutputNameList = stageOutputNameList :+ outputColsName
  }

  /**
    * Function is adding features to the list for Vector Assembler stage
    * It scans the inputs and get the origin features for each input list
    *
    * @param inputColsName  : Input Columns names
    * @param outputColsName : Output Column name
    * @param dfSchema       : DataFrame schema
    */
  def addFeatureToVAList(inputColsName: Array[String], outputColsName: String, dfSchema: StructType): Unit = {
    var stageAVInputNameList = new ArrayBuffer[String](0)
    var stageAVInputTypeList = new ArrayBuffer[OpType](0)
    for (inputNameElements <- inputColsName) {
      var elementExists = false
      for (nameElementsIndex <- stageOutputNameList.indices) {
        if (stageOutputNameList(nameElementsIndex) == inputNameElements) {
          elementExists = true
          stageAVInputNameList = stageAVInputNameList ++ stageInputNameList(nameElementsIndex)
          stageAVInputTypeList = stageAVInputTypeList ++ stageInputTypeList(nameElementsIndex)
        }
      }
      if (!elementExists) {
        if (dfSchema.fields(dfSchema.fieldIndex(inputNameElements)).dataType == StringType) {
          //TODO: Char, Binary, DateTime?
          stageAVInputTypeList = stageAVInputTypeList :+ OpType.CATEGORICAL
        }
        else {
          stageAVInputTypeList = stageAVInputTypeList :+ OpType.CONTINUOUS
        }
        stageAVInputNameList = stageAVInputNameList :+ inputNameElements
      }
    }
    stageOutputNameList = stageOutputNameList :+ outputColsName
    stageInputNameList = stageInputNameList :+ stageAVInputNameList
    stageInputTypeList = stageInputTypeList :+ stageAVInputTypeList
  }

  /**
    * Function is slicing the features for the last stage if it was a vector slices or ChiSq selector
    *
    * @param numSlicedFeatures : number of sliced features
    * @param selectedFeatures  : Selected features Array
    * @param pcaExists         : If PCA exists then we don't slice because the features are reflecting all features
    */
  def SliceElements(numSlicedFeatures: Int, selectedFeatures: Array[Int], pcaExists: Boolean): Unit = {
    if (!pcaExists) {
      var vectorType = Array.fill(numSlicedFeatures)(OpType.CONTINUOUS)
      var vectorCsName = new ArrayBuffer[String]()
      for (cSFeatures <- selectedFeatures) {
        vectorCsName = vectorCsName :+ stageInputNameList.last(cSFeatures)
      }
      stageInputTypeList(stageInputTypeList.length - 1) = vectorType.to[ArrayBuffer]
      stageInputNameList(stageInputNameList.length - 1) = vectorCsName
    }
  }


  /**
    * Function is going through the pipeline stages and create the feature name/Type list for each stage
    *
    * @param pipelineModel   : Pipeline Model
    * @param pipelineLen     : Length of the pipeline
    * @param dfSchema        : DataFrame schema
    * @param featuresColName : features Column name
    */
  def parseFeatureNameType(pipelineModel: PipelineModel,
                           pipelineLen: Int,
                           dfSchema: StructType, featuresColName: String): Map[String, OpType] = {

    var pcaExists = false
    //Get feature mapping from pipeline stages
    for (stageIndex <- 0 until pipelineLen) {
      pipelineModel.stages(stageIndex) match {
        //TODO: Check when adding FE components which we don't support yet (word based FE)
        case i: VectorAssembler =>
          val stageModel = i
          addFeatureToVAList(inputColsName = stageModel.getInputCols, outputColsName = stageModel.getOutputCol, dfSchema)

        case i: PCAModel =>
          val stageModel = i
          pcaExists = true
          addFeatureToList(stageModel.getInputCol, stageModel.getOutputCol, dfSchema)

        case i: ChiSqSelectorModel =>
          val stageModel = i
          addFeatureToList(stageModel.getFeaturesCol, stageModel.getOutputCol, dfSchema)
          SliceElements(numSlicedFeatures = stageModel.getNumTopFeatures, selectedFeatures = stageModel.selectedFeatures, pcaExists)

        case i: VectorSlicer =>
          val stageModel = i
          addFeatureToList(stageModel.getInputCol, stageModel.getOutputCol, dfSchema)
          SliceElements(numSlicedFeatures = stageModel.getIndices.length, selectedFeatures = stageModel.getIndices, pcaExists)

        case i: StringIndexerModel =>
          val stageModel = i
          addFeatureToList(stageModel.getInputCol, stageModel.getOutputCol, dfSchema, inputType = Some(OpType.CATEGORICAL))

        case i: VectorIndexerModel =>
          val stageModel = i
          addFeatureToList(stageModel.getInputCol, stageModel.getOutputCol, dfSchema)
          if (!pcaExists) {
            var vectorType = Array.fill(stageModel.numFeatures)(OpType.CONTINUOUS)
            for (mapElements <- stageModel.categoryMaps) {
              vectorType(mapElements._1) = OpType.CATEGORICAL
            }
            stageInputTypeList(stageInputTypeList.length - 1) = vectorType.to[ArrayBuffer]
          }

        case i: RFormulaModel =>
          val stageModel = i
          val s1 = classOf[RFormulaModel].getDeclaredMethod("pipelineModel")
          s1.setAccessible(true)
          val rfrPipelineModel = s1.invoke(stageModel).asInstanceOf[PipelineModel]
          parseFeatureNameType(rfrPipelineModel, rfrPipelineModel.stages.length - 2, dfSchema, stageModel.getFeaturesCol)

        case _ =>
          val stageModel = pipelineModel.stages(stageIndex)
          val inputColParam = stageModel.getParam("inputCol")
          val outputColParam = stageModel.getParam("outputCol")
          addFeatureToList(stageModel.get(inputColParam).get.toString, stageModel.get(outputColParam).get.toString, dfSchema)
      }
    }

    //Get the features names and types of the featureColName
    //TODO how to cover the case for the final list when there are repeating elements
    var finalNameList = new ArrayBuffer[String]()
    var finalTypeList = new ArrayBuffer[OpType]()
    for (nameElementsIndex <- stageOutputNameList.indices) {
      if (stageOutputNameList(nameElementsIndex) == featuresColName) {
        finalNameList = stageInputNameList(nameElementsIndex)
        finalTypeList = stageInputTypeList(nameElementsIndex)
      }
    }
    finalNameList.zip(finalTypeList).toMap
  }
}
