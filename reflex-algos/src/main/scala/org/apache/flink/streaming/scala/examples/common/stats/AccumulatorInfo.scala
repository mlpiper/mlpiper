package org.apache.flink.streaming.scala.examples.common.stats

import breeze.linalg.{DenseMatrix, DenseVector}
import com.parallelmachines.reflex.common.InfoType.InfoType
import org.mlpiper.utils.ParsingUtils

import scala.collection.mutable

/**
  * Case class to hold the count and current value of an accumulator.
  *
  * @param value          It represents what shall go to accumulator
  * @param count          Count is to keep track of updates
  * @param accumGraphType Datatype of value or whatever inside value is - SCALAR, TEXT, etc.
  * @param accumModeType  Category of accumulator - TIME_SERIES, INSTANT
  */
case class AccumulatorInfo[T](value: T,
                              count: Long = 1L,
                              accumGraphType: AccumData.GraphType,
                              accumModeType: AccumMode.AccumModeType,
                              customMap: Option[mutable.Map[String, String]] = None,
                              name: String,
                              infoType: InfoType,
                              modelId: String = null,
                              timestamp_ns: String = null)
  extends Serializable {

  private val _timestamp_ns = timestamp_ns match {
    case null => (System.currentTimeMillis() * 1e+6).toLong.toString
    case _ => timestamp_ns
  }

  //  format :
  //    {
  //      "data"      : "String representation of data",
  //      "mode"      : "TIMESERIES",
  //      "timestamp" : "1508446540113",
  //      "graphType" : "BARGRAPH",
  //      "name" : "categoricalHistogram",
  //      "type" : "Health"
  //    }
  override def toString: String = {
    val accumulatorValuesMap: mutable.Map[String, String] = mutable.Map[String, String](
      AccumulatorInfoJsonHeaders.DataKey -> AccumValueStringCreator.toString(value, key = Some(name)),
      AccumulatorInfoJsonHeaders.AccumGraphTypeKey -> accumGraphType.toString,
      AccumulatorInfoJsonHeaders.AccumModeKey -> accumModeType.toString,
      AccumulatorInfoJsonHeaders.TimeStampKey -> _timestamp_ns,
      AccumulatorInfoJsonHeaders.NameKey -> name,
      AccumulatorInfoJsonHeaders.InfoTypeKey -> infoType.name,
      AccumulatorInfoJsonHeaders.ModelIdKey -> modelId
    )

    // TODO verify if the custom map doesnt overwrite our own fields
    ParsingUtils.iterableToJSON(accumulatorValuesMap ++ customMap.getOrElse(mutable.Map[String, String]()))
  }
}

/**
  * Object holds the Accumulator Json keys
  */
object AccumulatorJsonHeaders {
  val NameKey = "name"
  val ValueKey = "value"
}

/**
  * Object holds AccumulatorInfo Json keys
  */
object AccumulatorInfoJsonHeaders {
  val DataKey = "data"
  val TimeStampKey = "timestamp"
  val AccumModeKey = "mode"
  val AccumGraphTypeKey = "graphType"
  val InfoTypeKey = "type"
  val NameKey = "name"
  val ModelIdKey = "modelId"
}

/**
  * Object holds enumerations of supportive categories of accumulators.
  * Right now we support two basic categories - TIME_SERIES and INSTANT
  */
object AccumMode extends Enumeration {
  type AccumModeType = Value
  val TimeSeries: Value = Value("TIME_SERIES")
  val Instant: Value = Value("INSTANT")

  def contains(accumMode: String): Boolean = values.exists(_.toString == accumMode)

  override def toString: String = s"Supported Accumulator Modes Are: ${values.mkString(", ")}"
}

/**
  * Object holds enumerations of supportive datatypes in accumulators.
  * It can be SCALAR(number), TEXT, MAP, VECTOR, etc.
  */
object AccumData extends Enumeration {
  type GraphType = Value
  val Unknown: Value = Value("UNKNOWN")

  val Opaque: Value = Value("OPAQUE")

  val GeneralGraph: Value = Value("GENERAL_GRAPH")

  val Vector: Value = Value("VECTOR")

  val Matrix: Value = Value("MATRIX")

  val Histogram: Value = Value("HISTOGRAM")

  val Heatmap: Value = Value("HEATMAP")

  val BarGraph: Value = Value("BARGRAPH")

  val KPI: Value = Value("KPI")

  val LineGraphs: Value = Value("LINEGRAPH")

  val MultiLineGraph: Value = Value("MULTILINEGRAPH")

  def contains(accumGraphType: String): Boolean = values.exists(_.toString == accumGraphType)

  override def toString: String = s"Supported Accumulator Graph Types Are: ${values.mkString(", ")}"

  /** Method can ease up in deciding the datatype */
  def getGraphType[T](data: T): GraphType = {
    data match {
      case _: scala.Int =>
        LineGraphs
      case _: scala.Double =>
        LineGraphs
      case _: scala.Long =>
        LineGraphs
      case _: scala.Float =>
        LineGraphs
      case _: scala.Short =>
        LineGraphs

      case _: scala.Predef.String =>
        KPI

      case _: scala.collection.Map[_, _] =>
        MultiLineGraph

      case _: scala.Iterable[_] =>
        MultiLineGraph

      case _: scala.Array[_] =>
        MultiLineGraph

      case _: breeze.linalg.DenseVector[_] =>
        MultiLineGraph

      case _: breeze.linalg.DenseMatrix[_] =>
        Matrix

      case _ =>
        Unknown
    }
  }
}

/**
  * Object is responsible for correctly generating data information.
  * It provide toString method which should be called with value and method will correctly create String representation.
  */
object AccumValueStringCreator {
  def toString[T](value: T, key: Option[String] = None): String = {
    val dataKey = key.getOrElse("0")

    val mapOfValue: mutable.Map[String, Any] = mutable.Map[String, Any]()
    value match {
      case intV: scala.Int =>
        mapOfValue(dataKey) = intV

      case doubleV: scala.Double =>
        mapOfValue(dataKey) = doubleV

      case longV: scala.Long =>
        mapOfValue(dataKey) = longV

      case floatV: scala.Float =>
        mapOfValue(dataKey) = floatV

      case shortV: scala.Short =>
        mapOfValue(dataKey) = shortV

      case stringV: scala.Predef.String =>
        mapOfValue(dataKey) = stringV

      case mapV: scala.collection.Map[Any@unchecked, Any@unchecked] =>
        return accumMapToJSON(mapV)

      case iterableV: scala.Iterable[Any] =>
        return arrayToStringRep(iterableV.toArray)

      case arrayV: scala.Array[_] =>
        return arrayToStringRep(arrayV)

      case dv: breeze.linalg.DenseVector[_] =>
        dv.data match {
          case _: Array[Double] =>
            return ParsingUtils.breezeDenseVectorToJsonMap(dv.asInstanceOf[DenseVector[Double]])
          case _ =>
            return dv.toString
        }

      case dm: breeze.linalg.DenseMatrix[_] =>
        // currently supporting DM of Doubles
        dm.data match {
          case _: Array[Double] =>
            return ParsingUtils.breezeDenseMatrixToJsonMap(matrix = dm.asInstanceOf[DenseMatrix[Double]])
          case _ =>
            return dm.toString()
        }

      case other =>
        // returning toString of object for all other datatypes/wrappers
        return other.toString
    }

    ParsingUtils.iterableToJSON(mapOfValue)
  }

  /** Method is used to convert string into following format.
    *
    * For array of Int=> "[1.0,2.0]"
    * For array of String=> "[a,b]"
    */
  private def arrayToStringRep(array: Array[_]): String = {
    val attrNames = array.indices.toArray.map(_.toString)

    ParsingUtils.iterableToJSON((attrNames zip array).toMap)
  }

  /**
    * Method is used to convert map with column labels to JSON
    *
    * for Map[1 -> "2", 3 -> "4"]
    * JSON will look as => {"metadata":"{\"rowLabel\":\"\",\"columnLabel\":\"1,3\"}","data":"[2,4]"}
    */
  def accumMapToJSON(map: scala.collection.Map[Any, Any])
  : String = {
    ParsingUtils.iterableToJSON(map)
  }

  /**
    * Method is used to convert denseMatrix with row and column labels to JSON
    *
    * for matrix = DenseMatrix(BreezeDenseVector(1.0, 2.0), BreezeDenseVector(3.0, 4.0), BreezeDenseVector(5.0, 6.0))
    * rowLabel = Array("row1", "row2", "row3") & colLabel = Array("col1", "col2")
    * JSON will look as => {"data":"[1.0,2.0],[3.0,4.0],[5.0,6.0]","metadata":"{\"rowLabel\":\"row1,row2,row3\",\"columnLabel\":\"col1,col2\"}"}
    */
  def accumMatrixToJSON(matrix: DenseMatrix[Double],
                        rowLabel: Option[Array[String]] = None,
                        columnLabel: Option[Array[String]] = None)
  : String = {
    val arrayOfRowLabel = if (rowLabel.isEmpty) {
      val rowRange = 0 until matrix.rows

      rowRange.toArray.map(_.toString)
    }
    else {
      rowLabel.get
    }

    val arrayOfColLabel = if (columnLabel.isEmpty) {
      val colsRange = 0 until matrix.cols

      colsRange.toArray.map(_.toString)
    }
    else {
      columnLabel.get
    }

    val arrayOfRowString = new Array[Any](matrix.rows)
    for (rowIndexOfMatrix <- 0 until matrix.rows) {
      val columnValues = matrix(rowIndexOfMatrix, ::).inner.toArray

      arrayOfRowString(rowIndexOfMatrix) =
        (arrayOfColLabel zip columnValues).toMap
    }

    ParsingUtils.iterableToJSON((arrayOfRowLabel zip arrayOfRowString).toMap)
  }
}
