package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import com.parallelmachines.reflex.pipeline._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class SaveToFile extends SparkBatchComponent {
  override val isSource: Boolean = false
  override val group: String = ComponentsGroups.sinks
  override val label: String = "RDD to File"
  override val description: String = "Save the RDD to a text file"
  override val version: String = "1.0.0"

  private val input1 = ComponentConnection(
    tag = typeTag[RDD[String]],
    label = "String",
    description = "Data to save to file",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList(input1)
  override var outputTypes: ConnectionList = ConnectionList.empty()

  val filePath = ComponentAttribute("filepath", "", "File Name", "Specify the file path to use as output. " +
    "Valid schemes are 'file:///' and 'hdfs:///'")
  attrPack.add(filePath)

  /** Generate the DAG portion of the specific component and the specific engine
    *
    * @param env          Flink environment
    * @param dsArr        Array of DataStream[Any]
    * @param errPrefixStr Error prefix string to use when errors happens during the run of the DAG component
    * @return
    */
  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    val rdd = dsArr(0).data[RDD[String]].repartition(1)

    var fileToWrite = filePath.value
    var timeStampDir = System.currentTimeMillis().toString
    if (!filePath.value.endsWith("/")) {
      timeStampDir = "/" + timeStampDir
    }

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
    rdd.saveAsTextFile(fileToWrite)

    ArrayBuffer[DataWrapperBase]()
  }
}
