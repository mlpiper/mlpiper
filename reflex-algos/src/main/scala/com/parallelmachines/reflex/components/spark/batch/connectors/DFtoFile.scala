package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.components.spark.batch.algorithms.SparkMLSink
import com.parallelmachines.reflex.components.{ComponentAttribute, SeparatorComponentAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

class DFtoFile extends SparkMLSink with DFSinkCommon {
  override val label: String = "DataFrame to CSV"
  override val description: String = "Save DataFrame to a formatted file"
  override val version: String = "1.0.0"

  val filePath = ComponentAttribute("filepath", "", "File Name", "Path to folder where output will be saved. Valid schemes are 'file:///' and 'hdfs:///'")
  val separator = SeparatorComponentAttribute()
  val withHeaders = ComponentAttribute("withHeaders", true, "With Headers", "Save dataframe with headers or not. (Default: true)", optional= true)

  //TODO: add format parameter with list of supported formats, REF-3322

  attrPack.add(filePath,separator, withHeaders)

  override def doSink(sc: SparkContext, df: DataFrame): Unit = {
    var fileToWrite = filePath.value
    val header = withHeaders.value
    val sep = separator.value
    val frmt = "csv"

    var timeStampDir = System.currentTimeMillis().toString
    if (!filePath.value.endsWith("/")) {
      timeStampDir = "/" + timeStampDir
    }

    val dropCols = colsToDrop(df)

    // files will be written inside <provided directory>/<time stamp> as shown below

    // localhost /tmp/xyz -  $ ls -R
    //1525966628445
    //
    // ./1525966628445:
    //_SUCCESS	part-00000

    fileToWrite = fileToWrite + timeStampDir

    /**
      * on yarn with save functionality works as following
      * 1. with simple path (/tmp/xyz) - it saved to hdfs
      * 2. with hdfs://simple path - it saved to hdfs
      * 3. But with file://simple path -- it saves to temporary folder with success message! Problem may be because of nonshared directly structure! (weird! )
      * i.e. <simple_path>/_temporary/0/task_20180509144536_0018_m_000000 ...
      * But there is nothing we can do about it!
      */

    df.drop(dropCols:_*)
      .write
      .format(frmt)
      .option("header", header)
      .option("sep", sep)
      .save(fileToWrite)
  }
}
