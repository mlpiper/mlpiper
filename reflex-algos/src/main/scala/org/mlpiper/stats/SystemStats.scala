package org.mlpiper.stats

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
