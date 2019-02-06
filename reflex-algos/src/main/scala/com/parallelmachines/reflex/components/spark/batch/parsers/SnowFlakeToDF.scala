package com.parallelmachines.reflex.components.spark.batch.parsers

import com.parallelmachines.reflex.common.SnowFlakeCommon
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import com.parallelmachines.reflex.pipeline._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.DataFrame

class SnowFlakeToDF extends SnowFlakeCommon {

  override val isSource: Boolean = true
  override val group: String = ComponentsGroups.connectors
  override val label: String = "SnowFlake to DataFrame"
  override val description: String = "Reads Snowflake table and converts it to Spark DataFrame"
  override val version: String = "1.0.0"

  private val output = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  val query = ComponentAttribute("query", "", "query", "The exact query (SELECT statement) to run.")

  attrPack.add(query)
  override val inputTypes: ConnectionList = ConnectionList.empty()
  override var outputTypes: ConnectionList = ConnectionList(output)

  def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {

    val spark: SparkSession = SparkSession.builder.master(env.getConf.get("spark.master")).getOrCreate
    val sqlContext = spark.sqlContext

    val sfOp = getSnowFlakeParams()

    val q = query.value

    val df: DataFrame = sqlContext.read.format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOp)
      .option("query", q)
      .load()

    //TODO remove the print once sf connector has stabilized
    df.printSchema()
    val sparkPipelineInfo = new SparkBatchPipelineInfo(df)
    ArrayBuffer(DataWrapper(sparkPipelineInfo))
  }
}
