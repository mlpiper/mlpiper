package org.mlpiper.infrastructure

import com.parallelmachines.reflex.common.ReflexEvent.ReflexEvent.EventType
import ConnectionGroups.ConnectionGroups

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.reflect.runtime.universe._

/**
  * forwardEvent flag shows that ReflexEvent object is expected on this connection,
  * so singleton components will not wrap/extract it into ReflexEvent, but just forward it,
  **/
case class EventTypeInfo(eventType: EventType, eventLabel: Option[String] = None, forwardEvent: Boolean = false)

object EventDescs {

  object Anomaly extends EventTypeInfo(EventType.Anomaly)

  object Model extends EventTypeInfo(EventType.Model)

  object ModelEventRepeat extends EventTypeInfo(EventType.Model, None, true)

  object MLHealthModel extends EventTypeInfo(EventType.MLHealthModel)

  object Alert extends EventTypeInfo(EventType.Alert)

  object CanaryHealth extends EventTypeInfo(EventType.CanaryHealth)

  object ModelReceived extends EventTypeInfo(EventType.ModelAccepted, None, true)

}

case class ComponentConnection[T](tag: TypeTag[T],
                                  defaultComponentClass: Option[Class[_]] = None,
                                  var sideComponentClass: Option[Class[_]] = None,
                                  eventTypeInfo: Option[EventTypeInfo] = None,
                                  label: String = "",
                                  description: String = "",
                                  group: ConnectionGroups = ConnectionGroups.OTHER,
                                  isVisible: Boolean = true) {

  def defaultComponentName: Option[String] = defaultComponentClass match {
    case Some(classParam) => Some(classParam.getSimpleName)
    case None => None
  }

  def sideComponentName: Option[String] = sideComponentClass match {
    case Some(classParam) => Some(classParam.getSimpleName)
    case None => None
  }

  override def toString: String = {
    s"{type: ${tag.tpe.toString} default: $defaultComponentName side: $sideComponentName label: $label description: $description group: ${group.toString}}"
  }

  def toJsonable: ListMap[String, Any] = {
    val listMap = mutable.ListMap[String, Any](
      "type" -> tag.tpe.toString,
      "defaultComponent" -> defaultComponentName,
      "label" -> label,
      "description" -> description,
      "group" -> group.toString
    )
    if (!isVisible) {
      listMap("isVisible") = isVisible
    }

    /* Converting mutable ListMap to immutable */
    val builder = ListMap.newBuilder[String, Any]
    for (x <- listMap) {
      builder += x
    }
    builder.result()
  }

  /**
    * Check if this class can be the parent/feeder of the other class
    *
    * @param other Other class to check connection to
    * @tparam O Type parameter
    * @return True if this connection can feed data to the other connection
    */
  def canConnectTo[O](other: ComponentConnection[O]): Boolean = {
    this.tag.tpe <:< other.tag.tpe
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ComponentConnection[T] =>
        tag == that.tag
      case _ => false
    }
  }
}
