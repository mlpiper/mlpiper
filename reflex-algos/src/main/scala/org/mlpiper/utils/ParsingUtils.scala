package org.mlpiper.utils

import java.io.File

import breeze.linalg.{DenseMatrix, DenseVector}
import com.parallelmachines.reflex.common.DirectoryPack
import com.parallelmachines.reflex.common.enums.OpType
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.json4s.DefaultFormats
import org.json4s.jackson.{Json, JsonMethods}
import org.mlpiper.datastructures.{ColumnEntry, LabeledVector, NamedVector, NamedVectorUtils}
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Iterable, Map, mutable}

object ParsingUtils {


  private val LOG = LoggerFactory.getLogger(this.getClass)

  // ----------------------------------------------------------------------
  //  Array
  // ----------------------------------------------------------------------

  /**
    * Converts a vector represented as a [[String]] to a [[DenseVector]] of [[Double]]
    *
    * @param vectorString     String to parse
    * @param elementSeparator Char that separates each element
    * @return [[DenseVector]] of [[Double]] if the parsing was successful. None otherwise.
    */
  def stringToBreezeDenseVector(vectorString: String,
                                elementSeparator: Char)
  : Option[DenseVector[Double]] = {
    val doubleArray = stringToDoubleArray(vectorString, elementSeparator)
    if (doubleArray.isDefined) {
      Some(new DenseVector[Double](doubleArray.get))
    } else {
      None
    }
  }

  /**
    * Converts a vector represented as a [[String]] to an [[Array]] of Doubles.
    *
    * @param vectorString     String to parse
    * @param elementSeparator Char that separates each element
    * @return Array of Doubles if the parsing was successful. None otherwise.
    */
  private def stringToDoubleArray(vectorString: String,
                                  elementSeparator: Char)
  : Option[Array[Double]] = {
    try {
      Some(vectorString.trim.split(elementSeparator.toString, -1).map(_.trim.toDouble))
    } catch {
      case _: Exception => None
    }
  }

  def breezeDenseVectorToJsonMap(vector: DenseVector[Double],
                                 colLabel: Option[Array[String]] = None)
  : String = {
    val attrNames = if (colLabel.isEmpty || colLabel.get.length != vector.length) {
      vector.data.indices.toArray.map(_.toString)
    } else {
      colLabel.get
    }
    iterableToJSON((attrNames zip vector.data).toMap)
  }

  def namedVectorToJsonMap(vector: NamedVector)
  : String = {
    iterableToJSON(vector.toFeatureMap)
  }

  def iterableToJSON(jsonable: Iterable[_]): String = {
    Json(DefaultFormats).write(jsonable)
  }

  def breezeDenseMatrixToString(matrix: DenseMatrix[Double],
                                elementSeparator: Char): Option[String] = {
    val rows = matrix.rows

    if (rows == 0) {
      LOG.error("Zero row breeze matrix provided")
      return None
    }

    val mapOfDenseMatrix: mutable.Map[String, String] = mutable.Map[String, String]()

    for (rowIndexOfMatrix <- 0 until rows) {
      mapOfDenseMatrix(rowIndexOfMatrix.toString) = breezeDenseVectorToString(matrix(rowIndexOfMatrix, ::).inner, elementSeparator = elementSeparator).get
    }

    Some(mapToString(mapOfDenseMatrix, keyValueSeparator = ':', rowSeparator = elementSeparator))
  }


  // ----------------------------------------------------------------------
  //  DenseVector
  // ----------------------------------------------------------------------

  def breezeDenseVectorToString(vector: DenseVector[Double],
                                elementSeparator: Char)
  : Option[String] = {
    if (vector.length == 0) {
      LOG.error("Zero length breeze vector provided")
      return None
    }
    Some(doubleArrayToString(vector.activeValuesIterator, elementSeparator))
  }

  private def doubleArrayToString(doubleIterator: Iterator[Double],
                                  elementSeparator: Char)
  : String = {
    doubleIterator.mkString(elementSeparator.toString)
  }

  // ----------------------------------------------------------------------
  //  LabeledVector
  // ----------------------------------------------------------------------

  def mapToString(map: Map[_, _], keyValueSeparator: Char, rowSeparator: Char): String = {
    var stringRep = ""

    map.foreach(x => stringRep = stringRep + "{" + x._1 + keyValueSeparator + x._2 + "}" + rowSeparator)

    stringRep.dropRight(1)
  }

  def breezeDenseMatrixToJsonMap[T](matrix: DenseMatrix[T],
                                    rowLabel: Option[Array[String]] = None,
                                    colLabel: Option[Array[String]] = None)
  : String = {

    val matrixOfStrings: DenseMatrix[String] = matrix.map(_.toString())
    val rowNames = if (rowLabel.isEmpty || rowLabel.get.length != matrixOfStrings.rows) {
      (0 until matrixOfStrings.rows).toArray.map(_.toString)
    } else {
      rowLabel.get
    }
    val colNames = if (colLabel.isEmpty || colLabel.get.length != matrixOfStrings.cols) {
      (0 until matrixOfStrings.cols).toArray.map(_.toString)
    } else {
      colLabel.get
    }

    val arrayOfRowString = new Array[Any](matrixOfStrings.rows)
    for (rowIndexOfMatrix <- 0 until matrixOfStrings.rows) {
      val columnValues = matrixOfStrings(rowIndexOfMatrix, ::).inner.toArray

      val colTuples = colNames zip columnValues
      var arrayColMap = ListMap[String, String]()
      colTuples.foreach(colTuple => arrayColMap += (colTuple._1 -> colTuple._2))
      arrayOfRowString(rowIndexOfMatrix) = arrayColMap
    }

    val rowTuples = rowNames zip arrayOfRowString
    var arrayRowMap = ListMap[String, Any]()
    rowTuples.foreach(rowTuple => arrayRowMap += (rowTuple._1 -> rowTuple._2))
    iterableToJSON(arrayRowMap)
  }

  def stringToLabeledVector(labeledVectorInput: String,
                            elementSeparator: Char,
                            labelSeparator: Option[Char] = None,
                            timestampSeparator: Option[Char] = None)
  : Option[LabeledVector[Double]] = {
    var label: Option[Double] = None
    var timestamp: Option[Long] = None

    var labeledVectorString: String = labeledVectorInput
    var timestampString: String = null
    var labelString: String = null

    if (labelSeparator.isDefined) {
      val labelSplit = labeledVectorString.split(labelSeparator.get)
      if (labelSplit.length != 2) {
        LOG.error("Failed to extract a label from " + labeledVectorString)
        return None
      }

      labelString = labelSplit(0)
      labeledVectorString = labelSplit(1)

      try {
        label = Some(labelString.trim.toDouble)
      } catch {
        case _: NumberFormatException =>
          LOG.error("Could not parse label " + labelString + " to a double")
          return None
      }
    }

    if (timestampSeparator.isDefined) {
      val timestampSplit = labeledVectorString.split(timestampSeparator.get)
      if (timestampSplit.length != 2) {
        LOG.error("Failed to extract a timestamp from " + labeledVectorString)
        return None
      }

      labeledVectorString = timestampSplit(0)
      timestampString = timestampSplit(1)

      try {
        timestamp = Some(timestampString.trim.toLong)
      } catch {
        case _: NumberFormatException =>
          LOG.error("Could not parse timestamp " + timestampString + " to a long")
          return None
      }
    }

    stringToLabeledVector(labeledVectorString, label, timestamp, elementSeparator)
  }

  /**
    * Converts a vector represented as a [[String]] to a [[LabeledVector]] of [[Double]]
    *
    * @param vectorString     String to parse
    * @param label            Label
    * @param timestamp        Timestamp
    * @param elementSeparator Char that separates each element
    * @return [[LabeledVector]] of [[Double]] if the parsing was successful. None otherwise.
    */
  private def stringToLabeledVector(vectorString: String,
                                    label: Option[Double],
                                    timestamp: Option[Long],
                                    elementSeparator: Char)
  : Option[LabeledVector[Double]] = {
    val doubleArray = stringToDoubleArray(vectorString, elementSeparator)
    if (doubleArray.isDefined) {
      Some(new LabeledVector[Double](
        label, new DenseVector[Double](doubleArray.get), timestamp))
    } else {
      None
    }
  }

  def labeledVectorToString(labeledVector: LabeledVector[Double],
                            elementSeparator: Char,
                            labeledElementSeparator: Option[Char] = None,
                            timestampElementSeparator: Option[Char] = None,
                            debug: Boolean = false)
  : Option[String] = {
    var label: String = null
    var timestamp: String = null

    if (labeledVector.vector.length == 0) {
      LOG.error("Zero elements in the labeledVector")
      return None
    }

    if (labeledVector.hasLabel && labeledElementSeparator.isDefined) {
      label = labeledVector.label.toString + labeledElementSeparator.get.toString
    }
    if (labeledVector.hasTimestamp && timestampElementSeparator.isDefined) {
      timestamp = timestampElementSeparator.get.toString + labeledVector.timestamp.toString
    }

    var output: String = labeledVector.vector.data.mkString(elementSeparator.toString)

    if (label != null) {
      output = label + output
    }
    if (timestamp != null) {
      output = output + timestamp
    }

    Some(output)
  }

  /**
    * Converts a matrix represented as a [[DenseMatrix]] to a [[String]]
    *
    * @param denseMatrix      Matrix to convert
    * @param elementSeparator Char that separates each element
    * @param rowSeparator     Char that separates each row
    * @return [[String]] String representing the matrix delimited by elementSeparator
    */
  def denseMatrixToString(denseMatrix: DenseMatrix[Double],
                          elementSeparator: Char,
                          rowSeparator: Char): String = {
    val rows = denseMatrix.rows
    val flatMatrix = denseMatrix.toDenseVector.data
    Range(0, rows).map(row => Range(0, flatMatrix.length, step = rows).map(col => flatMatrix(row + col))) // column->row
      .map(row => row.mkString(elementSeparator.toString)).mkString(rowSeparator.toString) // create string
  }

  /**
    * Converts a matrix represented as a [[String]] to a [[DenseMatrix]]
    *
    * @param denseMatrixStr   String to parse
    * @param elementSeparator Char that separates each element
    * @param rowSeparator     Char that separates each row
    * @return [[DenseMatrix]] if the parsing was successful. None otherwise.
    */
  def denseMatrixFromString(denseMatrixStr: String,
                            elementSeparator: Char,
                            rowSeparator: Char): Option[DenseMatrix[Double]] = {
    try {
      val rowStrArr = denseMatrixStr.split(rowSeparator).map(_.split(elementSeparator))
      val data = rowStrArr.flatten.map(_.toDouble)
      Some(DenseMatrix.create[Double](rowStrArr.length, rowStrArr(0).length,
        data, offset = 0, majorStride = rowStrArr(0).length, isTranspose = true))
    }
    catch {
      case _: Throwable =>
        LOG.error(s"Failed to parse matrix string $denseMatrixStr")
        None
    }
  }

  /**
    * Takes a packed directory input as a [[String]] and unpacks it to the local file system.
    * Returns pathname as a [[String]]
    *
    * @param inputString String that is a base64 encoded, packed directory
    * @return [[String]] pathname to the local directory if the parsing was successful. None otherwise.
    */
  def stringToLocalStore(inputString: String, baseDir: String)
  : Option[String] = {

    val uuid = java.util.UUID.randomUUID.toString
    val modelDir = new File(baseDir, uuid)

    if (!modelDir.mkdir()) {
      LOG.error("Failed to create directory " + modelDir.getPath)
      None
    }

    val packer = new DirectoryPack

    try {
      val unencodedModelByteArray = Base64.decodeBase64(inputString.getBytes())
      packer.unPack(unencodedModelByteArray, modelDir.getPath)
      Some(modelDir.getPath)
    } catch {
      case ex: Exception =>
        LOG.error("stringToLocalStore failed ", ex)
        FileUtils.deleteDirectory(modelDir)
        None
    }
  }

  def breezeDenseMatrixToJSON(matrix: DenseMatrix[Double]): String = {
    val rows = matrix.rows
    val mapOfDenseMatrix: mutable.Map[String, String] = mutable.Map[String, String]()

    for (rowIndexOfMatrix <- 0 until rows) {
      mapOfDenseMatrix(rowIndexOfMatrix.toString) = breezeDenseVectorToJSON(matrix(rowIndexOfMatrix, ::).inner)
    }

    iterableToJSON(mapOfDenseMatrix)
  }

  def breezeDenseVectorToJSON(vector: DenseVector[Double]): String = {
    Json(DefaultFormats).write(vector.toArray)
  }

  // ----------------------------------------------------------------------
  // Named Vectors
  // ----------------------------------------------------------------------

  def namedVectorToDenseVector(rVector: NamedVector): Option[DenseVector[Double]] = {
    val doubleArray: Array[Double] = new Array[Double](rVector.vector.length)
    for ((columnEntry: ColumnEntry, idx: Int) <- rVector.vector.zipWithIndex) {
      if (!columnEntry.columnValue.isInstanceOf[Double]) {
        LOG.error("Received a Vector containing an element that cannot be cast as a double")
        return None
      }
      doubleArray(idx) = columnEntry.columnValue.asInstanceOf[Double]
    }
    Some(new DenseVector[Double](doubleArray))
  }

  // ----------------------------------------------------------------------
  //  JSON representations
  // ----------------------------------------------------------------------

  def stringToNamedVector(input: String,
                          labelIndex: Int,
                          labelName: String,
                          columnNames: Array[String],
                          separator: String): Option[NamedVector] = {

    val rowElements: Array[String] = input.split(separator)
    var columnEntries: ArrayBuffer[ColumnEntry] = new ArrayBuffer[ColumnEntry]()
    val rVector: NamedVector = NamedVector(Array[ColumnEntry](), None, Some(System.currentTimeMillis()))
    var intOpt: Option[Int] = None
    var longOpt: Option[Long] = None
    var doubleOpt: Option[Double] = None
    var decimalOpt: Option[scala.math.BigDecimal] = None
    var columnEntry: ColumnEntry = null
    var labelEntry: Option[Any] = None

    for (((columnName: String, rowElement: String), i: Int) <- (columnNames zip rowElements).zipWithIndex) {
      intOpt = tryIntParse(rowElement)
      if (intOpt.isEmpty) {
        longOpt = tryLongParse(rowElement)
        if (longOpt.isEmpty) {
          doubleOpt = tryDoubleParse(rowElement)
          if (doubleOpt.isEmpty) {
            decimalOpt = tryDecimalParse(rowElement)
            if (decimalOpt.isEmpty) {
              columnEntry = ColumnEntry(columnName, rowElement, OpType.CONTINUOUS)
              columnEntries += columnEntry
            } else {
              columnEntry = ColumnEntry(columnName, decimalOpt.get, OpType.CONTINUOUS)
              columnEntries += columnEntry
            }
          } else {
            columnEntry = ColumnEntry(columnName, doubleOpt.get, OpType.CONTINUOUS)
            columnEntries += columnEntry
          }
        } else {
          columnEntry = ColumnEntry(columnName, longOpt.get, OpType.CONTINUOUS)
          columnEntries += columnEntry
        }
      }
      else {
        columnEntry = ColumnEntry(columnName, intOpt.get, OpType.CONTINUOUS)
        columnEntries += columnEntry
      }
      if (i == labelIndex) {
        labelEntry = Some((columnEntry.columnName, columnEntry.columnValue))
        columnEntries = columnEntries.slice(0, columnEntries.length)
      }
    }
    rVector.vector = columnEntries.toArray
    rVector.label = labelEntry
    Some(rVector)
  }

  def tryIntParse(item: String): Option[Int] = {
    try {
      Some(item.toInt)
    } catch {
      case _: Throwable => None
    }
  }

  def tryLongParse(item: String): Option[Long] = {
    try {
      Some(item.toLong)
    } catch {
      case _: Throwable => None
    }
  }

  // ----------------------------------------------------------------------
  // Named Vectors
  // ----------------------------------------------------------------------

  def tryDoubleParse(item: String): Option[Double] = {
    try {
      Some(item.toDouble)
    } catch {
      case _: Throwable => None
    }
  }

  def tryDecimalParse(item: String): Option[scala.math.BigDecimal] = {
    try {
      Some(item.asInstanceOf[scala.math.BigDecimal])
    } catch {
      case _: Throwable => None
    }
  }

  def jsonToNamedVector(jsonStr: String): Option[NamedVector] = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    val jValOpt = JsonMethods.parseOpt(jsonStr)
    if (jValOpt.isEmpty) {
      LOG.warn("Could not parse json")
      None
    }

    try {
      val columnNames = (jValOpt.get \ "columnNames").extract[Array[String]]
      val columnValues = (jValOpt.get \ "values").extract[Array[Any]]

      val label = (jValOpt.get \ "label").extractOrElse[String]("")

      val columnEntries = NamedVectorUtils.createColumnEntries(columnNames, columnValues)

      val rVec = NamedVector(columnEntries, None, Some(System.currentTimeMillis()))

      val labelEntry = rVec.getColumnEntryFromColumnName(label)
      if (labelEntry.isDefined) {
        rVec.label = Some((labelEntry.get.columnName, labelEntry.get.columnValue))
      }
      Some(rVec)
    } catch {
      case e: Throwable =>
        LOG.error(e.getMessage)
        None
    }
  }

  /** API provides conversion from LabeledVector to NamedVector */
  def labeledVectorToNamedVector(input: LabeledVector[Double]): Option[NamedVector] = {
    val namedVectorFromDV = denseVectorToNamedVector(input = input.vector)

    if (namedVectorFromDV.isEmpty) {
      None
    } else {
      val labelOption = if (input.hasLabel) {
        Some(input.label)
      } else {
        None
      }

      Some(NamedVector(vector = namedVectorFromDV.get.vector, label = labelOption))
    }
  }

  /** API provides conversion from DenseVector to NamedVector */
  def denseVectorToNamedVector(input: DenseVector[Double]): Option[NamedVector] = {
    if (input.length == 0) {
      None
    } else {
      val columnEntry: Array[ColumnEntry] = new Array[ColumnEntry](input.length)

      val colsName = DataFrameUtils.produceNames(input.length)

      input.toArray.zip(colsName).zipWithIndex.foreach(f = eachZippedInput => {
        columnEntry(eachZippedInput._2) =
          ColumnEntry(columnName = eachZippedInput._1._2,
            columnValue = eachZippedInput._1._1,
            columnType = OpType.CONTINUOUS)
      })

      Some(NamedVector(vector = columnEntry))
    }
  }

  def rowToNamedVector(row: Row,
                       colMap: Option[Map[String, OpType]]): NamedVector = {

    if (colMap.isDefined) {
      val columnEntry: Array[ColumnEntry] = new Array[ColumnEntry](colMap.get.size)

      val featuresAndOpType: Map[String, OpType] = colMap.get
      val colsName: Iterator[String] = colMap.get.keysIterator

      colsName.zipWithIndex.foreach(eachFeatureAndIndex => {
        val rowValue = row.get(row.fieldIndex(eachFeatureAndIndex._1))
        columnEntry(eachFeatureAndIndex._2) =
          ColumnEntry(
            columnName = eachFeatureAndIndex._1,
            columnValue = rowValue,
            featuresAndOpType(eachFeatureAndIndex._1))
      })

      NamedVector(vector = columnEntry, label = None, timeStamp = None)
    } // return empty column entry vector in case of missing colMaps
    else {
      val columnEntry: Array[ColumnEntry] = new Array[ColumnEntry](0)

      NamedVector(vector = columnEntry, label = None, timeStamp = None)
    }
  }

  def namedVectorToRow(namedVector: NamedVector): Row = {
    Row.fromSeq(namedVector.vector.map(_.columnValue))
  }
}
