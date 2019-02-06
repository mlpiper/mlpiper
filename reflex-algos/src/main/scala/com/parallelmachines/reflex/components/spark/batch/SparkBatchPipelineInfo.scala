package com.parallelmachines.reflex.components.spark.batch

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

/**
  * SparkBatchPipelineInfo is designed to keep Spark Pipeline stages added
  * by Reflex components during pipeline creation.
  *
  * Result Pipeline Model must hold all the transformation stages,
  * thus fit must be called only once in the very end.
  *
  */
class SparkBatchPipelineInfo(val dataframe: DataFrame) {

  val pipeline = new Pipeline().setStages(Array[PipelineStage]())
  // currentDataFrameCols keeps track of current cols in added to DF for given stages when accessed
  private var currentDataFrameCols: Array[String] = getDataframeColumns

  var transformedDF: Option[DataFrame] = None

  /** Updates current dataframe cols based on provided selectSpecificCols, addCols and removeCols.
    * operation order - set, add and remove
    *
    * @param setSpecificCols For selecting specific column names in given pipeline info.
    * @param addCols         For adding columns names for each pipeline stage.
    * @param removeCols      For removing columns names for each pipeline stage.
    */
  def updateCurrentDataFrameCols(setSpecificCols: Option[Array[String]],
                                 addCols: Option[Array[String]],
                                 removeCols: Option[Array[String]]): Unit = {
    // override currentDataFrameCols if selectSpecificCols is provided
    if (setSpecificCols.isDefined) {
      val setSpecificColsArray: Array[String] = setSpecificCols.get

      val currentDataFrameColsBuffer: ArrayBuffer[String] = ArrayBuffer[String]()

      setSpecificColsArray.foreach(eachNeededSpecificCols =>
        if (currentDataFrameCols.contains(eachNeededSpecificCols)) {
          currentDataFrameColsBuffer += eachNeededSpecificCols
        }
      )

      currentDataFrameCols = currentDataFrameColsBuffer.toArray
    }

    // adding cols to currentDataFrameCols if addCols is provided
    if (addCols.isDefined) {
      val addedColsArray: Array[String] = addCols.get

      val currentDataFrameColsBuffer: ArrayBuffer[String] = ArrayBuffer(currentDataFrameCols: _*)

      currentDataFrameColsBuffer ++= addedColsArray

      currentDataFrameCols = currentDataFrameColsBuffer.toArray
    }

    // removing cols from currentDataFrameCols if removeCols is provided
    if (removeCols.isDefined) {
      val removedColsArray: Array[String] = removeCols.get

      val currentDataFrameColsBuffer: ArrayBuffer[String] = ArrayBuffer(currentDataFrameCols: _*)

      currentDataFrameColsBuffer --= removedColsArray

      currentDataFrameCols = currentDataFrameColsBuffer.toArray
    }

    currentDataFrameCols = currentDataFrameCols.distinct
  }

  /** Adding stage as well as adding support for adding, removing or specifing column names while adding stages.
    *
    * @param setSpecificCols For selecting specific column names in given pipeline info.
    * @param addCols         For adding columns names for each pipeline stage.
    * @param removeCols      For removing columns names for each pipeline stage.
    */
  def addStage(stage: PipelineStage,
               setSpecificCols: Option[Array[String]] = None,
               addCols: Option[Array[String]] = None,
               removeCols: Option[Array[String]] = None): Unit = {
    val newStages = pipeline.getStages :+ stage
    pipeline.setStages(newStages)

    updateCurrentDataFrameCols(setSpecificCols = setSpecificCols, addCols = addCols, removeCols = removeCols)
  }

  private def getDataframeColumns: Array[String] = {
    dataframe.columns
  }

  /** For accessing current DF columns. */
  def getCurrentDataframeColumns: Array[String] = {
    this.currentDataFrameCols
  }

  def fit(): PipelineModel = {
    pipeline.fit(dataframe)
  }

  def setTransformedDataframe(newDF: DataFrame): Unit = {
    this.transformedDF = Some(newDF)
  }

  def getTransformedDataframe(): Option[DataFrame] = {
    this.transformedDF
  }
}
