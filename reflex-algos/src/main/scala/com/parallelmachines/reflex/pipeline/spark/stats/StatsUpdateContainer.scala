package com.parallelmachines.reflex.pipeline.spark.stats

import scala.collection.mutable


/**
  * The purpose of this class is to define placeholders for Spark statistics update. Each update
  * includes the stage id, task id and metrics as a map of <metric-name> -> <value>.
  * Values can be Long, Float, Boolean, String
  */

case class StatsUpdateContainer(stageId: Long, taskId: Long, metrics: mutable.HashMap[String, Any])

