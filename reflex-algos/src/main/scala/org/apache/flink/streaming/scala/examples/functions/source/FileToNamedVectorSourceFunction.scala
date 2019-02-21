package org.apache.flink.streaming.scala.examples.functions.source

import java.nio.file.{Files, Path, Paths}
import java.util.function.Consumer

import com.parallelmachines.reflex.pipeline.DataFrameUtils
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.scala.examples.clustering.math.ReflexNamedVector
import org.apache.flink.streaming.scala.examples.clustering.utils.ParsingUtils

/** This class converts the input text stream into a labeled dense vector of Doubles */
class FileToNamedVectorSourceFunction(file: String,
                                      labelIndex: Int,
                                      separator: String,
                                      header: Boolean,
                                      debug: Boolean = false)
  extends RichParallelSourceFunction[ReflexNamedVector] {

  require(Files.exists(Paths.get(file)), s"Must have $file present!")

  override def run(ctx: SourceFunction.SourceContext[ReflexNamedVector]) {

    // Get header. Otherwise, name the columns feature_1, ...
    var i = 0
    var labelName = ""
    var columnNames: Array[String] = Array()
    val filePath: Path = Paths.get(file)

    Files.lines(filePath).forEachOrdered(new Consumer[String]() {
      override def accept(dataLine: String) = {
        if(columnNames.length == 0){
          columnNames = header match {
            case true => dataLine.split(separator)
            case false => DataFrameUtils.produceNames(Range(0, dataLine.split(separator).length))
          }

          if(labelIndex > -1) {
            labelName = columnNames(labelIndex)
          }
        }

        if((header && i > 0) || (!header)) {
          val maybeNamedVector = ParsingUtils.stringToNamedVector(dataLine, labelIndex, labelName, columnNames, separator)
          if(maybeNamedVector.isDefined){
            ctx.collect(maybeNamedVector.get)
          }
        }
        i+=1
      }
    })

    ctx.close()
  }

  override def cancel(): Unit = { /* Do nothing. */ }
}
