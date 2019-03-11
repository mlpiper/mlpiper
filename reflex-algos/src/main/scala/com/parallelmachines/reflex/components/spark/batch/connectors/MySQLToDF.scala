package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.components.ComponentAttribute.ComponentAttribute
import com.parallelmachines.reflex.components._
import com.parallelmachines.reflex.components.spark.batch.{SparkBatchComponent, SparkBatchPipelineInfo}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

class MySQLToDF extends SparkBatchComponent {

  override val isSource: Boolean = true
  override val group: String = ComponentsGroups.connectors
  override val label: String = "MySQL to DataFrame"
  override val description: String = "Component is responsible for reading from MySQL Table & convert it to SparkML DataFrame"
  override val version: String = "1.1.0"

  private val output = ComponentConnection(
    tag = typeTag[SparkBatchPipelineInfo],
    label = "DataFrame",
    description = "DataFrame",
    group = ConnectionGroups.DATA)

  override val inputTypes: ConnectionList = ConnectionList.empty()
  override var outputTypes: ConnectionList = ConnectionList(output)

  val sqlHostName: ComponentAttribute[String] = SQLHostNameComponentAttribute()
  val sqlPort: ComponentAttribute[Int] = SQLHostPortComponentAttribute()
  val sqlDatabase: ComponentAttribute[String] = SQLDataBaseNameComponentAttribute()
  val sqlQuery: ComponentAttribute[String] = SQLQueryComponentAttribute()
  val sqlUsername: ComponentAttribute[String] = SQLUserNameComponentAttribute()
  val sqlPassword: ComponentAttribute[String] = SQLPasswordComponentAttribute()

  attrPack.add(sqlHostName, sqlPort, sqlDatabase, sqlQuery, sqlUsername, sqlPassword)

  def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    // checking if driver packages are available
    // works with percona, mariaDB, and MySQL!
    val driverManager = "com.mysql.jdbc.Driver"
    Class.forName(driverManager)

    val hostName = sqlHostName.value
    val port = sqlPort.value
    val db = sqlDatabase.value
    val jdbcuri = s"jdbc:mysql://$hostName:$port/$db"

    // creating temp alias of query
    val table = s"(${sqlQuery.value}) temp_alias_${System.currentTimeMillis()}"

    val user = sqlUsername.value
    val password = sqlPassword.value

    val sparkReadDFFormat = "jdbc"

    val spark: SparkSession =
      SparkSession
        .builder
        .master(env.getConf.get("spark.master"))
        .getOrCreate

    val df: DataFrame =
      spark
        .read
        .format(sparkReadDFFormat)
        .option("driver", driverManager)
        .option("url", jdbcuri)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .load()

    val sparkPipelineInfo = new SparkBatchPipelineInfo(df)

    ArrayBuffer(DataWrapper(sparkPipelineInfo))
  }
}
