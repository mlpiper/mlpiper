package com.parallelmachines.reflex.components.spark.batch.parsers

import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.components.{ComponentAttribute, SeparatorComponentAttribute}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.mlpiper.infrastructure._
import org.mlpiper.utils.DataFrameUtils

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class CsvToDF extends SparkBatchComponent {

  override val isSource: Boolean = true
  override val group: String = ComponentsGroups.connectors
  override val label: String = "CSV to Dataframe"
  override val description: String = "Reads csv file and converts it to SparkML DataFrame"
  override val version: String = "1.0.0"

  private val output = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList.empty()
  override var outputTypes: ConnectionList = ConnectionList(output)

  val separator = SeparatorComponentAttribute()
  val withHeaders = ComponentAttribute("withHeaders", true, "With Headers", "Whenever CSV contains headers. If not, all fields will be renamed using 'c0, c1,...' pattern.", optional = false)
  val filePath = ComponentAttribute("filepath", "", "File Name", "Read data from a given path, line by line.")
  val ignoreLeadingWhiteSpace = ComponentAttribute("ignoreLeadingWhiteSpace", true, "Ignore Leading Whitespaces", "Ignore leading whitespaces in headers. (Default: true)", optional = true)
  val ignoreTrailingWhiteSpace = ComponentAttribute("ignoreTrailingWhiteSpace", true, "Ignore Trailing Whitespaces", "Ignore trailing whitespaces in headers. (Default: true)", optional = true)

  attrPack.add(separator, filePath, withHeaders, ignoreLeadingWhiteSpace, ignoreTrailingWhiteSpace)

  def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {

    val header = withHeaders.value
    val file = filePath.value
    val sep = separator.value
    val ignLeadWSpaces = ignoreLeadingWhiteSpace.value
    val ignTrailWSpaces = ignoreTrailingWhiteSpace.value

    /** In case when headers are read from file itself, check that they are not duplicate. */

    // REF-3025
    // Commented out due to an issue with running on Yarn, where the file is provided as file:/// or hdfs:///
    //    if (header) {
    //      val originalHeaders = Source.fromFile(file).getLines().next().split(sep)
    //      val distinctHeaders = originalHeaders.distinct
    //      val dupHeaders = originalHeaders.diff(distinctHeaders).distinct
    //      require(dupHeaders.length == 0, s"Headers '${dupHeaders.mkString(",")}' are duplicated in $file")
    //    }

    val spark: SparkSession = SparkSession.builder.master(env.getConf.get("spark.master")).getOrCreate
    val dfReader = spark.sqlContext.read.option("inferSchema", value = true)
      .option("header", header)
      .option("sep", sep)
      .option("ignoreLeadingWhiteSpace", ignLeadWSpaces)
      .option("ignoreTrailingWhiteSpace", ignTrailWSpaces)

    if (spark.version.contains("2.2.0")) {
      dfReader.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
    }

    var df = dfReader.csv(file)

    if (!header) {
      df = DataFrameUtils.renameColumns(df)
    }

    val sparkPipelineInfo = new SparkBatchPipelineInfo(df)

    ArrayBuffer(DataWrapper(sparkPipelineInfo))
  }
}
