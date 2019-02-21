package org.apache.flink.streaming.scala.examples.clustering.math

import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils
import org.apache.flink.streaming.scala.examples.common.algorithm.Prediction
import com.parallelmachines.reflex.common.enums.OpType
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object ColumnEntryCompareVal extends Enumeration {
  type ColumnEntryCompareVal = Value
  val ColumnEntriesSame: Value = Value(0)
  val ColumnEntriesDifferentName: Value = Value(-1)
  val ColumnEntriesDifferentVal: Value = Value(-2)
}

import org.apache.flink.streaming.scala.examples.clustering.math.ColumnEntryCompareVal._

// TODO: [REF-994] - ReflexVector Kafka Streaming or change data type
case class ReflexPrediction(labelData: Option[Any],
                            data: Array[ReflexColumnEntry],
                            score: Double,
                            timestamp: Option[Long] = None) extends Serializable with Prediction {

  override def toString: String = {
    val (label, score, timeStamp, dataStr) = prettyPrint()
    s"($label, $score, $dataStr, $timeStamp)"
  }

  def toJson: String = {
    val (label, score, timeStamp, dataStr) = prettyPrint()
    val json = ("label", label.toString) ~
      ("score", score.toString) ~
      ("timestamp", timeStamp.toString) ~
      ("data", dataStr)

    compact(render(json)).toString
  }

  private def prettyPrint(): (Any, Double, Long, String) = {
    val label = labelData.getOrElse("No label")
    val timeStamp = timestamp.getOrElse(-1L)
    val dataStr = data.indices.map(i => data(i).toString).mkString(", ")

    (label, score, timeStamp, dataStr)
  }

  def compare(that: ReflexPrediction): Boolean = {
    if (this.labelData.isEmpty != that.labelData.isEmpty) {
      // must both have predictions
      return false
    }
    if (this.labelData.nonEmpty) {
      if (!this.labelData.get.toString.equals(that.labelData.get.toString)) {
        return false
      }
    }
    if (this.score != that.score) {
      return false
    }
    ReflexVector.compareColumnEntryArray(this.data, that.data)
  }
}


case class ReflexColumnEntry(var columnName: String, columnValue: Any, columnType: OpType)
  extends Serializable {
  override def toString: String = {
    s"($columnName, $columnValue)"
  }

  def toTupleOfNameValue: (String, Any) = {
    (columnName, columnValue)
  }

  /**
    * 0 means that the column entries are the same
    * -1 means that the column names are different
    * -2 means that the column values don't match
    */
  def compare(that: ReflexColumnEntry): ColumnEntryCompareVal = {
    if (!this.columnName.equals(that.columnName)) ColumnEntriesDifferentName
    else if (!this.columnValue.equals(that.columnValue)) ColumnEntriesDifferentVal
    else ColumnEntriesSame
  }
}


case class ReflexNamedVector(var vector: Array[ReflexColumnEntry],
                             var label: Option[Any] = None,
                             var timeStamp: Option[Long] = None) extends Serializable {


  override def toString: String = {
    val columnsString = (0 until vector.length).map(i => vector(i).toString).mkString(",")
    s"($columnsString; $label; $timeStamp)"
  }

  def toFeatureMap: Map[String, Any] = {
    vector.map(columnEntry => columnEntry.toTupleOfNameValue).toMap
  }

  /** Method will return Named Vector which only contains Numeric Values Of All Continuous Features */
  def toContinuousNamedVector(dropNa: Boolean = false): ReflexNamedVector = {
    val vectorBuf = scala.collection.mutable.ArrayBuffer.empty[ReflexColumnEntry]

    vector.foreach(eachColEntry => {
      if (eachColEntry.columnType.equals(OpType.CONTINUOUS)) {
        try {
          val columnValue: Double =
            eachColEntry.columnValue match {
              case number: Number =>
                number.doubleValue()
              case _ =>
                if (eachColEntry.columnValue == null || eachColEntry.columnValue == None) {
                  Double.NaN
                } else {
                  // giving it a try for string too!
                  eachColEntry.columnValue.asInstanceOf[String].toDouble
                }
            }

          if (!columnValue.isNaN) {
            vectorBuf += ReflexColumnEntry(columnName = eachColEntry.columnName,
              columnValue,
              columnType = eachColEntry.columnType
            )
          } else if (!dropNa) {
            vectorBuf += ReflexColumnEntry(columnName = eachColEntry.columnName,
              columnValue,
              columnType = eachColEntry.columnType
            )

          } else {
            /* Do Nothing */
          }
        }
        catch {
          case _: Throwable => /* Do Nothing*/
        }
      }
    })

    ReflexNamedVector(
      vector = vectorBuf.toArray,
      label = this.label,
      timeStamp = this.timeStamp)
  }

  /** Method will return Named Vector which only contains Numeric Values Of All Categorical Features */
  def toCategoricalNamedVector(dropNa: Boolean = false): ReflexNamedVector = {
    val vectorBuf = scala.collection.mutable.ArrayBuffer.empty[ReflexColumnEntry]

    vector.foreach(eachColEntry => {
      try {
        if (eachColEntry.columnType.equals(OpType.CATEGORICAL)) {

          // dropNa | isActuallyNa  | !dropNa   | !isActuallyNa   | OR Between Both
          // T      | T             | F         | F               | F
          // T      | F             | F         | T               | T
          // F      | T             | T         | F               | T
          // F      | F             | T         | T               | T
          if (!dropNa || !(eachColEntry.columnValue == null).||(eachColEntry.columnValue == None)) {
            vectorBuf += ReflexColumnEntry(columnName = eachColEntry.columnName,
              columnValue = eachColEntry.columnValue,
              columnType = eachColEntry.columnType)
          }
        }
      } catch {
        case _: Throwable => /* Do Nothing*/
      }
    })

    ReflexNamedVector(
      vector = vectorBuf.toArray,
      label = this.label,
      timeStamp = this.timeStamp)
  }

  def getColumnEntryFromColumnName(columnName: String): Option[ReflexColumnEntry] = {
    for (columnEntry: ReflexColumnEntry <- vector) {
      if (columnEntry.columnName.equals(columnName)) {
        return Some(columnEntry)
      }
    }
    None
  }

  /** Method returns the named vector which contains selected cols names only */
  def selectCols(colsName: Set[String]): ReflexNamedVector = {
    val vectorBuf = scala.collection.mutable.ArrayBuffer.empty[ReflexColumnEntry]

    vector.foreach(eachColEntry => {
      if (colsName.contains(eachColEntry.columnName)) {
        vectorBuf += eachColEntry
      }
    })

    ReflexNamedVector(
      vector = vectorBuf.toArray,
      label = this.label,
      timeStamp = this.timeStamp)
  }

  /** Method can change column names in column entry based on provided array of new column names */
  def changeColumnNames(newColsName: Array[String])
  : ReflexNamedVector = {
    require(vector.length.equals(newColsName.length), "Size of new cols names provided does not match with actual column entries")

    vector.zip(newColsName).foreach(
      eachZippedColEntryAndNewColName => {
        eachZippedColEntryAndNewColName._1.columnName = eachZippedColEntryAndNewColName._2
      })

    this
  }

  def compare(that: ReflexNamedVector): Boolean = {
    val label1 = this.label
    val label2 = that.label

    if (label1.isDefined != label2.isDefined) {
      return false
    }

    if (label1.isDefined && !ReflexVector.compareAnyValues(label1.get.toString, label2.get.toString)) {
      return false
    }

    val timestamp1 = this.timeStamp
    val timestamp2 = that.timeStamp

    if (timestamp1.isDefined && timestamp2.isDefined) {
      if (Math.abs(timestamp1.get - timestamp2.get) > Math.pow(10, 7)) {
        return false
      }
    }

    val values1: Array[ReflexColumnEntry] = this.vector
    val values2: Array[ReflexColumnEntry] = that.vector
    ReflexVector.compareColumnEntryArray(values1, values2)
  }
}

object ReflexVector {
  /**
    * Parsing function for ReflexNamedVector.
    * Reciprocal of ReflexNamedVector's toString.
    *
    * ie. s"($columnsString; $label; $timeStamp)" -> ReflexNamedVector(columns, label, timeStamp)
    *
    * @param namedVectorStr String containing ReflexNamedVector
    */
  def fromString(namedVectorStr: String): Option[ReflexNamedVector] = {
    val splitRegex = "[(),]".r
    val split = splitRegex.split(namedVectorStr).filter(x => x != " " && x != "").map(_.replaceAll(" ", "").replaceAll(";", ""))
    val columnEntryArity = ReflexColumnEntry("", "", OpType.CATEGORICAL).productArity // must use arity of valid column entry type
    val splitLen = split.length

    if (splitLen < columnEntryArity) {
      // Invalid named vector string
      return None
    }

    var curIdx = splitLen - 1
    val timestampStr = split(curIdx) // None if not present
    curIdx -= 1 // move over timestamp

    val labelStr = if (timestampStr.contains("None")) {
      split(curIdx)
    } else {
      curIdx -= 1 // skip 'Some' token
      split(curIdx)
    }
    curIdx -= 1 // move over label

    var hadLabel = false
    val labelName = if (!labelStr.contains("None") && !split(curIdx).contains("Some")) {
      hadLabel = true
      split(curIdx)
    }
    else {
      "label"
    }
    curIdx -= 1 // move over labelName

    val columnEntriesStrArr = split.slice(0, curIdx).zipWithIndex
    val columnNames = labelName +: columnEntriesStrArr.filter(_._2 % 2 == 0).map(_._1)
    val columnValues = labelStr +: columnEntriesStrArr.filter(_._2 % 2 == 1).map(_._1)
    val labelIndex = 0
    val separator = ","
    val namedVectorOpt = ParsingUtils.stringToNamedVector(columnValues.mkString(separator),
      labelIndex, labelName, columnNames, separator)
    if (namedVectorOpt.isEmpty) return None
    val namedVector = namedVectorOpt.get
    namedVector.timeStamp = if (timestampStr.contains("None")) None else Some(timestampStr.toLong)
    namedVector.vector = namedVector.vector.slice(1, namedVector.vector.length)
    Some(namedVector)
  }

  def createColumnEntries(columnNames: Array[String],
                          columnValues: Array[Any]): Array[ReflexColumnEntry] = {
    require(columnNames.length == columnValues.length, s"columnName's length ${columnNames.length} does not match with provided column values' length ${columnValues.length}")
    val columnEntries: ArrayBuffer[ReflexColumnEntry] = new ArrayBuffer[ReflexColumnEntry]()

    for ((columnName, columnValue) <- columnNames zip columnValues) {
      // considering all will be always continuous
      columnEntries += ReflexColumnEntry(columnName, columnValue, OpType.CONTINUOUS)
    }

    columnEntries.toArray
  }

  def compareReflexPredictionSeq(seq1: Seq[ReflexPrediction], seq2: Seq[ReflexPrediction]): Boolean = {
    if (seq1.length != seq2.length) return false
    for ((x, y) <- seq1 zip seq2) {
      if (!x.compare(y)) {
        return false
      }
    }
    true
  }

  def compareReflexNamedVectorSeq(seq1: Seq[ReflexNamedVector], seq2: Seq[ReflexNamedVector]): Boolean = {
    if (seq1.length != seq2.length) return false
    for ((x, y) <- seq1 zip seq2) {
      if (!x.compare(y)) {
        return false
      }
    }

    true
  }

  def compareColumnEntryArray(arr1: Array[ReflexColumnEntry], arr2: Array[ReflexColumnEntry]): Boolean = {
    if (arr1.length != arr2.length) return false
    val arr1_sorted = arr1.sortBy(x => x.columnName)
    val arr2_sorted = arr2.sortBy(x => x.columnName)

    for ((x, y) <- arr1_sorted zip arr2_sorted) {
      if (!x.columnName.equals(y.columnName)) {
        return false
      }
      if (!ReflexVector.compareAnyValues(x.columnValue, y.columnValue)) {
        return false
      }
    }
    true
  }

  def compareAnyValues(label1: Any, label2: Any): Boolean = {
    val maybeLabel1Int = Try(label1.toString.toInt).getOrElse(None)
    val maybeLabel2Int = Try(label2.toString.toInt).getOrElse(None)
    if (maybeLabel1Int == maybeLabel2Int && maybeLabel1Int != None) return maybeLabel1Int == maybeLabel2Int

    val maybeLabel1Long = Try(label1.toString.toLong).getOrElse(None)
    val maybeLabel2Long = Try(label2.toString.toLong).getOrElse(None)
    if (maybeLabel1Long == maybeLabel2Long && maybeLabel1Long != None) return maybeLabel1Long == maybeLabel2Long

    val maybeLabel1Float = Try(label1.toString.toFloat).getOrElse(None)
    val maybeLabel2Float = Try(label2.toString.toFloat).getOrElse(None)
    if (maybeLabel1Float == maybeLabel2Float && maybeLabel1Float != None) return maybeLabel1Float == maybeLabel2Float

    val maybeLabel1Double = Try(label1.toString.toDouble).getOrElse(None)
    val maybeLabel2Double = Try(label2.toString.toDouble).getOrElse(None)
    if (maybeLabel1Double == maybeLabel2Double && maybeLabel1Double != None) return maybeLabel1Double == maybeLabel2Double

    val maybeLabel1Decimal = Try(new java.math.BigDecimal(label1.toString.replace(",", ""))).getOrElse(None)
    val maybeLabel2Decimal = Try(new java.math.BigDecimal(label2.toString.replace(",", ""))).getOrElse(None)
    if (maybeLabel1Decimal == maybeLabel2Decimal && maybeLabel1Decimal != None) return maybeLabel1Decimal == maybeLabel2Decimal

    label1.toString.equals(label2.toString)
  }
}
