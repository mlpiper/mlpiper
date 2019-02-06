package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.components.ComponentAttribute.ComponentAttribute
import com.parallelmachines.reflex.components._
import com.parallelmachines.reflex.components.spark.batch.algorithms.SparkMLSink
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame


class DFtoMySQL extends SparkMLSink {
  override val label: String = "DataFrame to MySQL"
  override val description: String = "Save DataFrame to MySQL"
  override val version: String = "1.1.0"

  val sqlHostName: ComponentAttribute[String] = SQLHostNameComponentAttribute()
  val sqlPort: ComponentAttribute[Int] = SQLHostPortComponentAttribute()
  val sqlDatabase: ComponentAttribute[String] = SQLDataBaseNameComponentAttribute()
  val sqlTable: ComponentAttribute[String] = SQLTableNameComponentAttribute()
  val sqlUsername: ComponentAttribute[String] = SQLUserNameComponentAttribute()
  val sqlPassword: ComponentAttribute[String] = SQLPasswordComponentAttribute()

  attrPack.add(sqlHostName, sqlPort, sqlDatabase, sqlTable, sqlUsername, sqlPassword)

  override def doSink(sc: SparkContext, df: DataFrame): Unit = {
    // checking if driver packages are available
    val driverManager = "com.mysql.jdbc.Driver"
    Class.forName(driverManager)

    val hostName = sqlHostName.value
    val port = sqlPort.value
    val db = sqlDatabase.value
    val jdbcuri = s"jdbc:mysql://$hostName:$port/$db"

    val table = sqlTable.value

    val user = sqlUsername.value
    val password = sqlPassword.value

    val sparkReadDFFormat = "jdbc"

    df.write
      .format(sparkReadDFFormat)
      .option("url", jdbcuri)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .option("driver", driverManager)
      .save
  }
}
