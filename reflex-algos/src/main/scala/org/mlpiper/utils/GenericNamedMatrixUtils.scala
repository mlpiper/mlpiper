package org.mlpiper.utils

import breeze.linalg.{DenseMatrix, DenseVector}
import com.parallelmachines.reflex.pipeline.DataFrameUtils
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.scala.examples.flink.utils.functions.window.SubtaskWindowBatch
import org.apache.spark.ml.PipelineModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.mlpiper.datastructures.{ColumnVectorEntry, NamedMatrix, NamedVector}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object GenericNamedMatrixUtils {

  //////////////////////////////////////////////////////////////////////////////////////
  // Supporting Definitions
  //////////////////////////////////////////////////////////////////////////////////////
  /** Method provides functionality to convert iterator of Named vector values to Named Vector which contains Matrix on each Partition. */
  def iteratorOfNamedVectorToNamedMatrix(iteratorOfNamedVector: Iterator[NamedVector]): NamedMatrix = {
    val arrayOfNamedVectors = iteratorOfNamedVector.toArray

    // creating map of column names and its values
    val columnValues: mutable.Map[String, ArrayBuffer[Any]] = mutable.Map[String, ArrayBuffer[Any]]()

    // iterating over whole batch to fill up columnValues's arrayBuffer entries
    arrayOfNamedVectors.foreach(eachNamedVector => {
      // filling up the columnValues
      eachNamedVector
        .vector
        .foreach(eachColEntryOfGivenVector => {
          if (columnValues.get(eachColEntryOfGivenVector.columnName).isEmpty) {
            columnValues(eachColEntryOfGivenVector.columnName) = ArrayBuffer.empty[Any]
          }
          columnValues(eachColEntryOfGivenVector.columnName) += eachColEntryOfGivenVector.columnValue
        })
    })

    // putting arrayed values in each column keys of columnValues
    val columnDensedValues = columnValues.map(x => (x._1, DenseVector(x._2.toArray)))
    val totalCols = columnDensedValues.size

    // here, reflex column entry array will represent whole matrix representation of the given batch. Where each columnValue will be array of type of columnType
    val reflexColumnVectorEntry: Array[ColumnVectorEntry] = new Array[ColumnVectorEntry](totalCols)

    // final step is to create reflexColumnEntry for reflex named vector
    columnValues.keys.zipWithIndex.foreach(eachColAndIndex => {
      reflexColumnVectorEntry(eachColAndIndex._2) =
        ColumnVectorEntry(columnName = eachColAndIndex._1,
          columnValue = columnDensedValues(eachColAndIndex._1))
    })

    NamedMatrix(arrayOfVector = reflexColumnVectorEntry)
  }

  /** Method provides functionality to convert denseMatrix to Named Vector which contains Matrix representation in columnar manner. */
  def denseMatrixDoubleToNamedMatrix(denseMatrix: DenseMatrix[Double])
  : NamedMatrix = {
    // tranposing matrix
    val transpose = denseMatrix.t

    // creating reflexColumnEntry to be size of rows (here, rows will be number of features)
    val reflexColumnEntry: Array[ColumnVectorEntry] = new Array[ColumnVectorEntry](transpose.rows)

    // since, metadata is missing, namesOfFeatures will be generated using produceNames functions
    val namesOfFeatures = DataFrameUtils.produceNames(transpose.rows)

    for (eachFeature <- 0 until transpose.rows) {
      // innerVector is vector for relevant row
      val innerVector: DenseVector[Double] = transpose(eachFeature, ::).inner

      reflexColumnEntry(eachFeature) = ColumnVectorEntry(columnName = namesOfFeatures(eachFeature), columnValue = innerVector)
    }

    NamedMatrix(arrayOfVector = reflexColumnEntry)
  }

  /** Method provides functionality to convert iterator of DenseVector vector values to Named Vector which contains Matrix on each Partition. */
  def iteratorOfDenseVectorDoubleToNamedMatrix(iteratorOfNamedVector: Iterator[DenseVector[Double]])
  : NamedMatrix = {
    val denseMatrix = BreezeDenseVectorsToMatrix.iteratorToMatrix(iteratorOfNamedVector)

    denseMatrixDoubleToNamedMatrix(denseMatrix = denseMatrix)
  }

  //////////////////////////////////////////////////////////////////////////////////////
  // Spark - Batch Applications
  //////////////////////////////////////////////////////////////////////////////////////
  /**
    * Method converts dataframe into RDD of ReflexNamedVector where each value in Column Entry will be array representing that feature.
    *
    * Transformation works in following way.
    *
    * <br> 1. If model is not provided, then using DataFrameUtils, whole dataframe will be converted to RDD of NamedVector when each cols will be initialized with default names. (i.e. c0, c1, ..)
    * <br> 2. If model is provided, then using inference's transform, dataframe will be converted to RDD of NamedVector.
    * <br> 3. RDD of NamedVector then will be converted to NamedVector which will represent NamedMatrix for given RDD partition.
    * <br> 4. Names in NamedVector(Matrix) cols will be changed if needed using changeColsName api.
    *
    * @param df           Input DataFrame
    * @param sparkMLModel Spark Model if it is provided
    */
  def createReflexNamedMatrix(df: DataFrame, sparkMLModel: Option[PipelineModel])
  : RDD[NamedMatrix] = {
    // create named vectors from Dataframe
    val rddOfNamedVector = DataFrameUtils.toRDDOfNamedVectorUsingSparkML(df = df, sparkMLModel = sparkMLModel, columnMap = None)

    val rddOfNamedMatrix = createReflexNamedMatrix(rddOfNamedVector = rddOfNamedVector)

    rddOfNamedMatrix
  }

  /** Method provides functionality to convert RDD of Named vector to RDD of Named Vector which contains Matrix on each Partition. */
  def createReflexNamedMatrix(rddOfNamedVector: RDD[NamedVector])
  : RDD[NamedMatrix] = {
    val reflexNamedMatrix =
      rddOfNamedVector
        // Map each partition's set of vectors into a reflex named matrix
        // Map partition always create iterator, even for empty data.
        // So adding conditions which handle empty iterator gracefully
        .mapPartitions(x => if (x.nonEmpty) Iterator(iteratorOfNamedVectorToNamedMatrix(x)) else Iterator.empty)

    reflexNamedMatrix
  }

  /** Method provides functionality to convert DataStream of NamedVector to DataStream of Named Vector which contains Matrix on each Partition with given window. */
  def createReflexNamedMatrix(dataStreamOfNamedVector: DataStream[NamedVector], windowingSize: Long)
  : DataStream[NamedMatrix] = {
    // creating denseMatrix on each subtask
    val streamOfIterableNamedVectors = SubtaskWindowBatch.countBatch(dataStreamOfNamedVector, windowingSize)

    streamOfIterableNamedVectors.map(eachRNV => iteratorOfNamedVectorToNamedMatrix(eachRNV.toIterator))
  }
}
