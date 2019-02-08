package com.parallelmachines.reflex.pipeline.spark.stats

import org.mlpiper.mlops.MLOpsEnvVariables
import com.parallelmachines.reflex.web.RestApis.buildURIPath
import com.parallelmachines.reflex.web.{RestApiName, RestClient}
import org.apache.spark.scheduler._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import org.json4s.DefaultFormats
import org.mlpiper.utils.ParsingUtils


class SystemStatsListener extends SparkListener {
  implicit val formats = DefaultFormats
  private val logger = LoggerFactory.getLogger(getClass)

  var client: Option[RestClient] = None
  val scheme = "http"

  if (MLOpsEnvVariables.agentRestHost.isDefined) {
    val portStr = MLOpsEnvVariables.agentRestPort.getOrElse("")
    val port = if (portStr != "") Some(portStr.toInt) else None

    client = Some(new RestClient(scheme, MLOpsEnvVariables.agentRestHost.get, port = port))
  } else {
    logger.warn("RestClient can not be initialized, agentRestHost field is empty")
  }

  val pipeId = MLOpsEnvVariables.pipelineInstanceId.getOrElse("")

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    super.onStageCompleted(stageCompleted)

    logger.debug("*** Stage complete *** " + stageCompleted.stageInfo.stageId)

    stageCompleted.stageInfo.accumulables.synchronized {
      val externalAccumulables = stageCompleted.stageInfo.accumulables.values
        .filter { acc => !acc.name.getOrElse("").startsWith("internal.") }

      if (externalAccumulables.nonEmpty) {
        /*
         * toListOfJsons filters out an absolute non-json values,
         * so mectrics may be empty
         */
        val metrics = AccumulatorStats.toListOfJsons(externalAccumulables)

        if (metrics.nonEmpty) {
          if (client.isDefined) {
            val params = Map[String, String]("statType" -> "accumulator")
            val uri = buildURIPath(RestApiName.stats.toString, pipeId)
            for (js <- metrics) {
              client.get.postString(uri, params, js)
            }
          }
        }
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    logger.debug("*** onTaskEnd ***")

    super.onTaskEnd(taskEnd)

    if (taskEnd == null) {
      logger.warn("Received SparkListenerTaskEnd null pointer!")
      return
    }

    if (taskEnd.taskMetrics == null) {
      logger.warn("Received SparkListenerTaskEnd.TaskMetrics null pointer!")
      return
    }

    var bytesRead: Long = 0
    var recordsRead: Long = 0

    if (taskEnd.taskMetrics.inputMetrics != null) {
      bytesRead = taskEnd.taskMetrics.inputMetrics.bytesRead
      recordsRead = taskEnd.taskMetrics.inputMetrics.recordsRead
    }

    var bytesWritten: Long = 0
    var recordsWritten: Long = 0

    if (taskEnd.taskMetrics.outputMetrics != null) {
      bytesWritten = taskEnd.taskMetrics.outputMetrics.bytesWritten
      recordsWritten = taskEnd.taskMetrics.outputMetrics.recordsWritten
    }

    val sysStatMap: mutable.HashMap[String, Any] = mutable.HashMap("sysstat.executionCpuTime" -> taskEnd.taskMetrics.executorCpuTime,
      "sysstat.executorRunTime" -> taskEnd.taskMetrics.executorRunTime,
      "sysstat.memoryBytesSpilled" -> taskEnd.taskMetrics.memoryBytesSpilled,
      "sysstat.bytesRead" -> bytesRead,
      "sysstat.recordsRead" -> recordsRead,
      "sysstat.bytesWritten" -> bytesWritten,
      "sysstat.recordsWritten" -> recordsWritten,
      "sysstat.stageId" -> taskEnd.stageId,
      "sysstat.taskId" -> taskEnd.taskInfo.taskId)

    if (client.isDefined) {
      val params = Map[String, String]("statType" -> "system")
      val uri = buildURIPath(RestApiName.stats.toString, pipeId)
      client.get.postString(uri, params, ParsingUtils.iterableToJSON(sysStatMap))
    }
  }
}
