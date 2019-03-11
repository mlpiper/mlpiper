package org.mlpiper.infrastructure

import java.util

import org.json4s.jackson.Serialization.write
import org.json4s.{JObject, _}
import org.mlpiper.infrastructure.ComputeEngineType.ComputeEngineType
import org.mlpiper.infrastructure.factory.ReflexComponentFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ReflexPipelineInfo {
  def apply(): ReflexPipelineInfo =
    new ReflexPipelineInfo("", ComputeEngineType.FlinkStreaming, None, ReflexSystemConfig(), mutable.ListBuffer[Component]())
}

/**
  * This is custom serialize.
  * First case is deserializer: (json -> object)
  * Second case is serializer: (object -> json)
  */
case object ComputeEngineTypeSerializer extends CustomSerializer[ComputeEngineType](format => ( {
  case JString(s) => ComputeEngineType.withName(s)
}, {
  case value: ComputeEngineType => JString(value.toString)
}))

/**
  * Serializer for Parent class.
  *
  * First case is deserializer: (json -> object)
  * Second case is serializer: (object -> json)
  *
  * ParentTypeSerializer format is used only in toJson method for serialization.
  * Parent class has a connectionTypeTag property to have resolve singleton connections,
  * but we don't wont to see this field in JSON.
  *
  */
case object ParentTypeSerializer extends CustomSerializer[Parent](format => ( {
  case JObject(pp) => throw new Exception("Parent deserializer is not implemented. This format shouldn't be used for deserialization.")
}, {
  /* Don't put connection tag into JSON */
  case value: Parent =>
    val lst = ListBuffer[(String, JsonAST.JValue)](("parent", JInt(value.parent)), ("output", JInt(value.output)))

    if (value.input.isDefined) {
      lst.append(("input", JInt(value.input.get)))
    }
    if (value.eventType.isDefined) {
      lst.append(("eventType", JString(value.eventType.get)))
    }
    if (value.eventLabel.isDefined) {
      lst.append(("eventLabel", JString(value.eventLabel.get)))
    }

    new JObject(lst.toList)
}))

case class ReflexPipelineInfo(var name: String = "ReflexPipeline",
                              var engineType: ComputeEngineType.ComputeEngineType,
                              var language: Option[String],
                              var systemConfig: ReflexSystemConfig,
                              pipe: mutable.ListBuffer[Component]) {
  def getMaxId: Int = {
    var maxId = 0
    if (pipe.nonEmpty) {
      maxId = pipe.map(_.id).max
    }
    maxId
  }

  def getPipeListAsJava(): util.Collection[Component] = {
    pipe.asJavaCollection
  }

  def addComponent(comp: Component): Unit = {
    require(!pipe.map(_.id).contains(comp.id), s"Id must be unique, pipeline already contains id: $getMaxId")
    pipe += comp
  }

  def toJson(): String = {
    implicit val formats = DefaultFormats + ComputeEngineTypeSerializer + ParentTypeSerializer
    write(this)
  }

  def getComponentById(id: Int): Option[Component] = {
    val filteredComponents = pipe.filter(_.id == id)

    require(filteredComponents.length <= 1, s"Pipeline has more that one component with id $id")
    if (filteredComponents.length == 0) {
      return None
    }
    Some(filteredComponents.head)
  }

  /**
    * Fetch list of components in a pipeline according to provided parameters
    *
    * @param compGroup             group of components to fetch
    * @param isSource              mark if components should be sources
    * @param outputConnectionGroup mark if output connection should be defined
    * @return List[Component]
    */
  //TODO: create case class Properties to pass as a one object instead of many parametes
  def getComponentsByProperties(compGroup: String, isSource: Boolean, outputConnectionGroup: Option[ConnectionGroups.ConnectionGroups] = None): List[Component] = {
    var compList = ListBuffer[Component]()
    for (comp <- pipe) {
      val compInstance = ReflexComponentFactory(engineType, comp.`type`, null)
      if ((compInstance.group == compGroup) && (compInstance.isSource == isSource)) {
        if (outputConnectionGroup.isEmpty) {
          compList += comp
        } else {
          //TODO: only first output is compared to provided param. (re-think)
          if (compInstance.outputTypes.nonEmpty && (compInstance.outputTypes.head.group == outputConnectionGroup.get)) {
            compList += comp
          }
        }
      }
    }
    compList.toList
  }

  /**
    * Fetch list of components in a pipeline according to provided parameters
    *
    * @param compGroup             group of components to fetch
    * @param isSource              mark if components should be sources
    * @param outputConnectionGroup mark if output connection should be defined
    * @return List[Component]
    */
  def getComponentsByPropertiesAsJava(compGroup: String, isSource: Boolean, outputConnectionGroup: ConnectionGroups.ConnectionGroups = null): java.util.List[Component] = {
    getComponentsByProperties(compGroup, isSource, if (outputConnectionGroup == null) None else Some(outputConnectionGroup)).asJava
  }

  /**
    * Fills canaryConfig section of systemConfig
    *
    * @param label1 label to mark 1st canary pipeline
    * @param label2 label to mark 2nd canary pipeline
    * @return ReflexPipelineInfo
    */
  def setCanaryLabels(label1: String, label2: String): ReflexPipelineInfo = {
    require(label1 != null || label2 != null, s"label1 and label2 are 'null', at least one of the labels must be provided")
    if (this.systemConfig.canaryConfig.isEmpty) {
      this.systemConfig.canaryConfig = Some(CanaryConfig(None, None))
    }
    this.systemConfig.canaryConfig.get.canaryLabel1 = Some(label1)

    if (label1 != null) {
      this.systemConfig.canaryConfig.get.canaryLabel1 = Some(label1)
    }
    if (label2 != null) {
      this.systemConfig.canaryConfig.get.canaryLabel2 = Some(label2)
    }
    this
  }
}
