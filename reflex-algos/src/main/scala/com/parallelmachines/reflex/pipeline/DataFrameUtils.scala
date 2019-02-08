package com.parallelmachines.reflex.pipeline

import breeze.linalg.DenseVector
import com.parallelmachines.reflex.common.SparkMLFeatureDetails
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.parallelmachines.reflex.common.enums.OpType
import org.mlpiper.datastructures.NamedVector
import org.mlpiper.utils.{GenericConstants, ParsingUtils}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Object [[DataFrameUtils]] is responsible for providing utilities related to spark's dataframe
  */
object DataFrameUtils {
  /**
    * Method will create new column by the name of `indexColumnName`. It will contain rowIDs.
    *
    * @param df              Dataframe that needs to be indexed
    * @param indexColumnName Name of the column which will contain rowID
    * @return Indexed dataframe
    */
  private def addColumnIndex(df: DataFrame, indexColumnName: String): DataFrame = {
    df
      .sqlContext
      .createDataFrame(
        // Add Column Index To RDD
        rowRDD = df.rdd.zipWithIndex.map {
          case (row, columnIndex) => Row.fromSeq(row.toSeq :+ columnIndex)
        },
        // Create New Schema Having ColumnIndex
        schema = StructType(df.schema.fields :+ StructField(indexColumnName, LongType, nullable = false))
      )
  }

  /**
    * Method will merge two dataframe's provided column name
    *
    * @param dataFrame1    First dataframe
    * @param columnNameOf1 Column name of DF1 that requires to be in new created DF, This column will be first column in new DF
    * @param dataFrame2    Second dataframe
    * @param columnNameOf2 Column name of DF2 that requires to be in new created DF, This column will be second column in new DF
    * @return Merged Dataframe
    */
  def mergeTwoDataFramesSingleColumn(dataFrame1: DataFrame,
                                     columnNameOf1: String,
                                     dataFrame2: DataFrame,
                                     columnNameOf2: String): DataFrame = {
    val nameOfColumnForIndex = "columnIndex"

    require(dataFrame1.schema.fieldNames.contains(columnNameOf1), s"Dataframe 1 does not contain $columnNameOf1 in schema")
    require(dataFrame2.schema.fieldNames.contains(columnNameOf2), s"Dataframe 2 does not contain $columnNameOf2 in schema")

    // indexing dataframe1 by appending rowID to each row data as new column provided by `nameOfColumnForIndex`
    val indexedDF1: DataFrame = addColumnIndex(dataFrame1.select(columnNameOf1), nameOfColumnForIndex)
    // indexing dataframe2 by appending rowID to each row data as new column provided by `nameOfColumnForIndex`
    val indexedDF2: DataFrame = addColumnIndex(dataFrame2.select(columnNameOf2), nameOfColumnForIndex)

    indexedDF1
      .join(indexedDF2, Seq(nameOfColumnForIndex))
      .drop(nameOfColumnForIndex)
  }

  def toDouble(df: DataFrame): DataFrame = {
    val toDouble = udf[Double, String](_.toDouble)

    var dfNew = df
    for (name <- df.columns) {
      dfNew = dfNew.withColumn(name, toDouble(dfNew(name)))
    }
    dfNew
  }

  /**
    * Method will produce c0, c1, .. up to range
    *
    * @param indices Range
    * @return Array of names
    */
  def produceNames(indices: Range): Array[String] = {
    (for (i <- indices) yield s"c$i").toArray
  }

  /**
    * Method will produce c0, c1, .. up to range
    *
    * @param size Size of cols names
    * @return Array of names
    */
  def produceNames(size: Int): Array[String] = {
    produceNames(0 until size)
  }

  /**
    * Method will give a header-less dataframe column names
    *
    * @param dataFrame DataFrame
    * @return Named Dataframe
    */
  def renameColumns(dataFrame: DataFrame): DataFrame = {
    val newNames = produceNames(dataFrame.columns.indices)
    dataFrame.toDF(newNames: _*)
  }

  /**
    * Converts all features of DataFrame to
    * RDD[DenseVector[Double]]
    *
    * @param df dataframe
    * @return RDD[DenseVector[Double]
    */
  def toRDDofDenseVectorDouble(df: DataFrame, defaultValue: Option[Double]): RDD[DenseVector[Double]] = {
    df.rdd.map { row =>
      var listDouble = ListBuffer[Double]()

      for (i <- 0 until row.size) {
        var value = 0.0
        var structField = row.schema.toList(i)
        structField.dataType match {
          case IntegerType => {
            value = row.getInt(i).toDouble
          }
          case DoubleType => {
            value = row.getDouble(i)
          }
          case x => {
            if (defaultValue.isDefined) {
              value = defaultValue.get
            } else {
              throw new Exception(s"Type $x is not supported")
            }
          }
        }
        listDouble += value
      }
      DenseVector(listDouble.toArray)
    }
  }

  /**
    * Converts all features of DataFrame to
    * RDD[ReflexNamedVector] using Spark ML Model.
    *
    * @note Please note that, named vector will not contain labels as for now it is not possible to access labels from dataFrame.
    * @param df dataframe
    * @return RDD[ReflexNamedVector]
    */
  def toRDDOfNamedVectorUsingSparkML(df: DataFrame,
                                     sparkMLModel: Option[PipelineModel],
                                     columnMap: Option[Map[String, OpType]]): RDD[NamedVector] = {

    // Before converting, it is important to check if sparkml model was available
    // if not available, then figure out which ones are categorical and continuous
    var colMap: Option[Map[String, OpType]] = None
    if (sparkMLModel.isEmpty) {
      if (columnMap.isDefined) {
        colMap = columnMap
      }
      else {
        var optypesArray = ArrayBuffer[OpType]()
        val exprs = df.columns.map(x => countDistinct(x).as(x))
        val size = exprs.length

        // 1. We assume that any column exceeding 25 (or MaximumCategoricalUniqueValue) unique
        // values are continuous data (and under 25 are categorical)
        // 2. The reason we slice the dataframe into 100 columns (or DataFrameColumnarSliceSize)
        // was that spark was consistently crashing when it tried to aggregate over a large number
        // of columns - the current value is empirical, unfortunately.
        for (i <- 0 until size by GenericConstants.DataFrameColumnarSliceSize) {
          val e2 = exprs.slice(i, math.min(i + GenericConstants.DataFrameColumnarSliceSize, size))
          val countsDF = df.agg(e2.head, e2.tail: _*).head(1)
          val array = countsDF(0)
            .toSeq
            .map(x => x.asInstanceOf[Long])
            .toArray
            .map(x => if (x > GenericConstants.MaximumCategoricalUniqueValue)
              OpType.CONTINUOUS else OpType.CATEGORICAL)
          optypesArray = optypesArray ++ array
        }

        optypesArray.zipWithIndex.foreach(x => println("%s = %s".format(df.columns(x._2), x._1)))

        colMap = Some(optypesArray.zipWithIndex.map(x => (df.columns(x._2), x._1)).toMap)
      }
    } else {
      val sparkMLFeatureDetails = new SparkMLFeatureDetails()
      val featuresDetails: Map[String, OpType] = sparkMLFeatureDetails.getFeatureCategory(sparkMLModel = sparkMLModel.get, dfSchema = df.schema)
      colMap = Some(featuresDetails)
    }

    val rddOfNamedVector =
      df.rdd.map(row => ParsingUtils.rowToNamedVector(row, colMap))

    rddOfNamedVector
  }

  /**
    * Method converts RDD[NamedVector] to DataFrame
    */
  def fromRDDOfNamedVector(rddOfNamedVector: RDD[NamedVector],
                           sc: SparkContext): DataFrame = {
    val sparkSession = SparkSession.builder()
      .config(sc.getConf)
      .getOrCreate()

    if (!rddOfNamedVector.isEmpty()) {
      // generating schema on top of first row only
      val featureNames = rddOfNamedVector.first().vector.map(_.columnName)
      val setOfFeatureNames = featureNames.toSet

      val featuresOpType = rddOfNamedVector.first().vector.map(_.columnType)

      // Create a schema for the dataframe
      var schema = new StructType()

      featureNames.zip(featuresOpType).foreach(eachFeaturesAndOpType => schema = schema.add(
        name = eachFeaturesAndOpType._1,
        dataType = if (eachFeaturesAndOpType._2.equals(OpType.CONTINUOUS)) {
          DoubleType
        } else {
          StringType
        },
        nullable = true))

      val rddOfRowContinuousValues: RDD[Row] = rddOfNamedVector.map(_.vector).map(eachColumnEntry => {
        val arrayOfContValues: Array[Any] = new Array[Any](featureNames.length)

        eachColumnEntry.foreach(columnEntry => {
          if (setOfFeatureNames.contains(columnEntry.columnName)) {
            val indexOfColumn = featureNames.indexOf(columnEntry.columnName)
            arrayOfContValues(indexOfColumn) = if (columnEntry.columnType.equals(OpType.CONTINUOUS)) {
              if (columnEntry.columnValue != None && columnEntry.columnValue != null) {
                columnEntry.columnValue.asInstanceOf[Double]
              } else {
                null.asInstanceOf[Double]
              }
            } else {
              if (columnEntry.columnValue != None && columnEntry.columnValue != null) {
                columnEntry.columnValue.asInstanceOf[String]
              } else {
                null.asInstanceOf[String]
              }
            }
          }
        })

        Row.fromSeq(arrayOfContValues)
      })

      val df = sparkSession.createDataFrame(rddOfRowContinuousValues, schema)

      df
    }
    else {
      sparkSession.emptyDataFrame
    }
  }
}
