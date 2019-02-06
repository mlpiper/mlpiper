package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.common.SnowFlakeCommon
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.spark.batch.algorithms.SparkMLSink
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode}

class DFToSnowFlake extends SparkMLSink with SnowFlakeCommon with DFSinkCommon {
  override val label: String = "DataFrame to Snowflake table"
  override val description: String = "Save DataFrame to a Snowflake table"
  override val version: String = "1.0.0"

  val sfTable = ComponentAttribute("sfTable", "", "Table", "Destination Table for the DataFrame.")
  val sfSaveMode = ComponentAttribute("sfSaveMode", "overwrite", "SaveMode",
    "SaveMode is used to specify the expected behavior of saving a DataFrame to a data source.",
    optional = true)
  sfSaveMode.setOptions(List[(String, String)](("append", "append"), ("overwrite", "overwrite"),
    ("ignore", "ignore"), ("error", "error")))

  attrPack.add(sfTable, sfSaveMode)

  override def doSink(sc: SparkContext, df: DataFrame): Unit = {
    val sfOp = getSnowFlakeParams()
    val table = sfTable.value

    val dropCols = colsToDrop(df)

    //TODO: Remove after we verify
    df.printSchema()

    val mode = sfSaveMode.value match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "ignore" => SaveMode.Ignore
      case "error" => SaveMode.ErrorIfExists
    }

    df.drop(dropCols:_*)
      .write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOp)
      .option("dbtable", table)
      .mode(mode)
      .save()
  }
}


