package org.apache.flink.streaming.scala.examples.flink.utils.functions.conversion

import com.parallelmachines.reflex.pipeline.DataFrameUtils
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector
import org.mlpiper.datastructures.NamedVector
import org.mlpiper.utils.ParsingUtils

/** This class converts the input text stream into a named vector */
class StringToNamedVectorFlatMap(labelIndex: Int,
                                 separator: String,
                                 debug: Boolean = false)
  extends RichFlatMapFunction[String, NamedVector] {

  var columnNames: Array[String] = Array()
  var labelName: String = ""

  private def createColumnNames(firstRow: Array[String]): Array[String] = {
    val rowLength = firstRow.length
    DataFrameUtils.produceNames(Range(0, rowLength))
  }

  override def flatMap(value: String, out: Collector[NamedVector]): Unit = {
    if (columnNames.length == 0) {
      columnNames = createColumnNames(value.split(separator))
      if (labelIndex > -1) labelName = columnNames(labelIndex)
    }

    val maybeNamedVector = ParsingUtils.stringToNamedVector(value, labelIndex, labelName, columnNames, separator)
    if (maybeNamedVector.isDefined) {
      out.collect(maybeNamedVector.get)
    }
  }
}
