package org.apache.flink.streaming.scala.examples.common.parsing

import java.sql.ResultSet
import java.time.Instant
import org.apache.flink.table.api.{Types => TableTypes}

object MySqlUtils {

  /* Flink Table API supported types */
  def resolveSqlColumnType(columnType: String): Option[_] = {
    columnType match {
      case "VARCHAR" => Some(TableTypes.STRING)
      case "BOOLEAN" => Some(TableTypes.BOOLEAN)
      case "TINYINT" => Some(TableTypes.BYTE)
      case "SMALLINT" => Some(TableTypes.SHORT)
      case "INT" => Some(TableTypes.INT)
      case "BIGINT" => Some(TableTypes.LONG)
      case "FLOAT" => Some(TableTypes.FLOAT)
      case "DOUBLE"|"REAL" => Some(TableTypes.DOUBLE)
      case "DECIMAL" => Some(TableTypes.DECIMAL)
      case "DATE" => Some(TableTypes.DATE)
      case "TIME" => Some(TableTypes.TIME)
      case "TIMESTAMP(3)" => Some(TableTypes.TIMESTAMP)
      case "INTERVAL YEAR TO MONTH" => Some(TableTypes.INTERVAL_MONTHS)
      case "INTERVAL DAY TO SECOND(3)" => Some(TableTypes.INTERVAL_MILLIS)
      case _ => None
    }
  }

  def resolveSqlColumnValue(columnName: String, tableColumnType: Any, resultSet: ResultSet): Option[_] = {
    tableColumnType match {
      case TableTypes.STRING => Some(resultSet.getString(columnName))
      case TableTypes.INT => Some(resultSet.getString(columnName).toInt)
      case TableTypes.FLOAT => Some(resultSet.getFloat(columnName))
      case TableTypes.DOUBLE => Some(resultSet.getDouble(columnName))
      case TableTypes.DECIMAL => Some(resultSet.getBigDecimal(columnName))
      case TableTypes.BOOLEAN => Some(resultSet.getBoolean(columnName))
      case TableTypes.BYTE => Some(resultSet.getByte(columnName))
      case TableTypes.LONG => Some(resultSet.getLong(columnName))
      case TableTypes.DATE => Some(Instant.parse(resultSet.getString(columnName)+"T00:00:00Z"))
      case TableTypes.TIME => Some(resultSet.getTime(columnName))
      case TableTypes.TIMESTAMP => Some(resultSet.getTimestamp(columnName))
      case _ => None
    }
  }
}


