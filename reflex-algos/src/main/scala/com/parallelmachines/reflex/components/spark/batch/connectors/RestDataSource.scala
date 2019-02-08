package com.parallelmachines.reflex.components.spark.batch.connectors

import org.mlpiper.mlops.MLOpsEnvVariables
import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import org.mlpiper.mlobject.Model
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import com.parallelmachines.reflex.pipeline.{ComponentsGroups, _}
import com.parallelmachines.reflex.web.RestApis
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/*
 * Data fetching source singleton component.
 *
 */
class RestDataSource extends SparkBatchComponent {
  override val isSource = true
  override val isSingleton = true
  override lazy val isVisible = false

  override val group = ComponentsGroups.connectors
  override val label = "RestDataSource"
  override val description = "Fetches data from rest"
  override val version = "1.0.0"

  override val inputTypes: ConnectionList = ConnectionList.empty()
  /**
    * outputTypes will not really be empty.
    * They will be filled dynamically according to connections, because component is Singleton
    */
  override var outputTypes: ConnectionList = ConnectionList.empty()

  private val timeoutMSEC = 1000

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {

    var retArray = mutable.ArrayBuffer[DataWrapperBase]()
    var requestData = true

    var health = List[String]()
    var model: Option[Model] = None

    /* Fetch last approved model and the health stats for it. */
    while (requestData) {
      model = RestApis.getLastApprovedModel(MLOpsEnvVariables.workflowInstanceId.get, MLOpsEnvVariables.pipelineInstanceId.get)

      if (model.isDefined) {
        requestData = false
        health = RestApis.getModelHealthStats(model.get)
        RestApis.postModelAccepted(model.get)
      }
      Thread.sleep(timeoutMSEC)
    }

    /* Iterate over output types, which will be defined by number of outgoing connections.
     * For now it is expected to be 2: model and health.
     */
    for (outputType <- outputTypes) {
      val eventTypeInfo = outputType.eventTypeInfo.get

      eventTypeInfo.eventType match {
        case ReflexEvent.EventType.Model =>
          retArray += new DataWrapper(model.get)
        case ReflexEvent.EventType.MLHealthModel =>
          retArray += new DataWrapper(env.parallelize(health))
      }
    }
    retArray
  }
}
