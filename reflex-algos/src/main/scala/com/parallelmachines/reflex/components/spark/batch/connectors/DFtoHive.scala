package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.components.spark.batch.algorithms.SparkMLSink
import com.parallelmachines.reflex.components.ComponentAttribute
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.mlpiper.infrastructure.SparkCapability


class DFtoHive extends SparkMLSink {
  override val label: String = "DataFrame to Hive"
  override val description: String = "Save DataFrame to Hive"
  override val version: String = "1.1.0"

  val schema = ComponentAttribute("schema", "", "Schema", "Schema To Use For Dumping DF")
  val table = ComponentAttribute("table", "", "Table", "Schema To Use For Dumping DF")

  attrPack.add(schema, table)

  override def doSink(sc: SparkContext, df: DataFrame): Unit = {
    if (SparkCapability.HiveSupport) {
      val writingSchema = schema.value
      val writingTable = table.value

      val spark: SparkSession =
        SparkSession
          .builder
          .master(sc.getConf.get("spark.master"))
          .getOrCreate

      // creating database
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $writingSchema")

      df.write.mode(SaveMode.Append).saveAsTable(s"$writingSchema.$writingTable")
    }
    else {
      throw new Exception(s"${this.label} component is trying to access Hive. But Hive capability is not enabled!")
    }
  }
}
