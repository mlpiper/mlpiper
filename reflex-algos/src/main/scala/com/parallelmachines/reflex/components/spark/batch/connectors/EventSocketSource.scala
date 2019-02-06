package com.parallelmachines.reflex.components.spark.batch.connectors

import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent
import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent
import com.parallelmachines.reflex.output.SocketSourceSingleton
import com.parallelmachines.reflex.pipeline.{ComponentsGroups, _}
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object ModelTypeString {
  var modelTypeString = true

  def setModelTypeString(modelTypeString1: Boolean): Unit = {
    modelTypeString = modelTypeString1
  }

  def getModelTypeString(): Boolean = {
    modelTypeString
  }
}

object SparkEventTypeFilterFunction {
  def filter(data: ReflexEvent, eventTypeInfo: EventTypeInfo): Boolean = {
    (data.eventType == eventTypeInfo.eventType) &&
      (data.eventLabel == eventTypeInfo.eventLabel)
  }
}

class EventSocketSource extends SparkBatchComponent {
  override val isSource = true
  override val isSingleton = true
  override lazy val isVisible = false

  override val group = ComponentsGroups.connectors
  override val label = "EventSocketSource"
  override val description = "Receives events from socket"
  override val version = "1.0.0"

  override val inputTypes: ConnectionList = ConnectionList.empty()
  /**
    * outputTypes will not really be empty.
    * They will be filled dynamically according to connections, because component is Singleton
    */
  override var outputTypes: ConnectionList = ConnectionList.empty()

  val mlObjectSocketHost = ComponentAttribute("mlObjectSocketHost", "", "Events source socket host", "Host to read ML health events from")
  val mlObjectSocketSourcePort = ComponentAttribute("mlObjectSocketSourcePort", -1, "Events source socket port", "Port to read ML health events from").setValidator(_ >= 0)

  attrPack.add(mlObjectSocketHost, mlObjectSocketSourcePort)

  override def materialize(env: SparkContext, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {

    var receivedEvents = ListBuffer[ReflexEvent]()

    var retArray = mutable.ArrayBuffer[DataWrapperBase]()
    val host = if (mlObjectSocketHost.value == "") null else mlObjectSocketHost.value
    val port = mlObjectSocketSourcePort.value
    var readFromSocket = true
    val ModelStringByte = ModelTypeString.getModelTypeString()


    val t = new java.util.Timer()
    val task = new java.util.TimerTask {
      def run() = {
        readFromSocket = false
        this.cancel()
      }
    }

    SocketSourceSingleton.startClient(host, port)

    while (readFromSocket) {
      var obj = SocketSourceSingleton.getRecord()
      if (obj != null) {
        receivedEvents += obj
        logger.debug("Got something on socket !!!")
        /** After model is received, wait 2 sec for Health Events */
        if (obj.eventType == ReflexEvent.EventType.Model) {
          logger.debug("Got model message")
          try {
            t.schedule(task, 2000L)
          } catch {
            /** This will be thrown when try to schedule task twice.*/
            case _: java.lang.IllegalStateException =>
          }
        }
      }
    }
    SocketSourceSingleton.stopClient()

    for (outputType <- outputTypes) {
      val eventTypeInfo = outputType.eventTypeInfo.get
      val filteredEventList = receivedEvents.filter(SparkEventTypeFilterFunction.filter(_, eventTypeInfo))

      /**
        * When connection is marked as forwardEvent, ReflexEvent object is expected to be received by component which is next in pipeline.
        * So data will not be extracted from the object, and it will be just forwarded.
        */
      if (eventTypeInfo.forwardEvent) {
        retArray += new DataWrapper(env.parallelize(filteredEventList))
      } else if (ModelStringByte) {
        // take only last
        retArray += new DataWrapper(env.parallelize(filteredEventList.map(event => new String(event.data.toByteArray))))
      } else{
        // take only last
        retArray += new DataWrapper(env.parallelize(filteredEventList.map(event => event.data.toByteArray)))
      }
    }
    retArray
  }
}