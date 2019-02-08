package com.parallelmachines.reflex.pipeline.spark.stats

/**
  * Spark system statistics are kept as a flat table in a task granularity. The task metrics
  * are saved whenever the Spark listener reports about them, and accumulated in a list. once
  * the statistics are read (upon REST request), the list is cleared thus sequenced reads
  * will not have overlapped data. (Alternatively, the clear operation could also be done using
  * dedicated REST API command, though, at this point, because there's a single reader, and
  * to keep it simple, this is done upon read operation.
  */

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.mlpiper.utils.ParsingUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SystemStats {

  private val tasksStats: ListBuffer[String] = new ListBuffer[String]()

  implicit val formats = DefaultFormats

  def addTaskStats(taskStats: mutable.HashMap[String, Any]): Unit = {
    tasksStats.synchronized {
      tasksStats += ParsingUtils.iterableToJSON(taskStats)
    }
  }

  def get(): String = {
    var json = ""
    tasksStats.synchronized {
      json = write(SystemStats.tasksStats)
      tasksStats.clear()
    }
    json
  }
}
