package org.mlpiper.infrastructure

import com.parallelmachines.reflex.common.enums.ComponentOrigin
import com.parallelmachines.reflex.components.ComponentAttributePack
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.runtime.universe._

object JsonHeaders {
  val NameHeader = "name"
  val EngineTypeHeader = "engineType"
  val Origin = "origin"
  val SystemConfigHeader = "systemConfigParameters"
  val GroupHeader = "group"
  val KeyHeader = "key"
  val TypeHeader = "type"
  val UITypeHeader = "uiType"
  val OptionalHeader = "optional"
  val DescriptionHeader = "description"
  val LabelHeader = "label"
  val VersionHeader = "version"
  val JsonVersionHeader = "jsonVersion"
  val DefaultValueHeader = "defaultValue"
  val ArgsHeader = "arguments"
  val InputInfoHeader = "inputInfo"
  val OutputInfoHeader = "outputInfo"
  val tagInfoHeader = "tags"
  val ComponentsHeader = "components"
  val ManifestHeader = "title"
  val ModelBehaviorHeader = "modelBehavior"
  val IsVisible = "isVisible"
}

object nameOf {
  def apply[T: TypeTag]: String = {
    typeOf[T].toString
  }
}


trait ReflexPipelineComponent extends Serializable {

  final lazy val logger = LoggerFactory.getLogger(this.getClass)
  val origin: ComponentOrigin = ComponentOrigin.BUILTIN
  val isSource: Boolean
  val isSingleton = false
  val isUserStandalone = false
  val isMlStatsUsed = false
  final lazy val name: String = this.getClass.getSimpleName
  val group: String
  val label: String
  val description: String
  val version: String
  val engineType: ComputeEngineType.Value
  val attrPack = ComponentAttributePack()
  val tagInfo: ListBuffer[String] = ListBuffer[String]() // Info about the tag of this components

  lazy val isVisible = true

  private val infoFieldsHash = mutable.Map[String, Any]()

  /** Provide description of the component arguments
    *
    * @return Map[String, String]
    */
  lazy val paramInfo: String = Json(DefaultFormats).write(attrPack.toJsonable())

  /**
    * Info about the input streams to this components
    *
    * @return
    */
  val inputTypes: ConnectionList

  /**
    * Output streams to this components
    *
    * @return
    */
  var outputTypes: ConnectionList


  protected def addInfoField(key: String, value: Any): Unit = {
    require(!infoFieldsHash.contains(key), s"Component info hash already contains field: $key")
    infoFieldsHash.put(key, value)
  }

  def buildInfo(): Unit = {
    addInfoField(JsonHeaders.NameHeader, name)
    addInfoField(JsonHeaders.EngineTypeHeader, engineType.toString)
    addInfoField(JsonHeaders.GroupHeader, group)
    addInfoField(JsonHeaders.LabelHeader, label)
    addInfoField(JsonHeaders.DescriptionHeader, description)
    addInfoField(JsonHeaders.VersionHeader, version)
    if (!isVisible) {
      addInfoField(JsonHeaders.IsVisible, isVisible)
    }
    addInfoField(JsonHeaders.ArgsHeader, "ARGUMENTS_PLACEHOLDER")
    addInfoField(JsonHeaders.InputInfoHeader, inputTypes.map(x => x.toJsonable))
    addInfoField(JsonHeaders.tagInfoHeader, tagInfo)
    addInfoField(JsonHeaders.OutputInfoHeader, outputTypes.map(x => x.toJsonable))
    addInfoField(JsonHeaders.Origin, origin.toString)
  }

  final def getInfo: String = {
    buildInfo()
    val jsonStr = Json(DefaultFormats).write(infoFieldsHash)

    /**
      * Tmp hack because it is very diffucult to change ArgumentParameterTool
      **/
    jsonStr.replaceFirst(s""""ARGUMENTS_PLACEHOLDER"""", paramInfo)
  }


  final def addTags(tag:String): Unit = {
    tagInfo.append(tag)
  }


  /** Configure the component
    *
    * @param paramMap Map containing key/value pairs for this component
    */
  def configure(paramMap: Map[String, Any]): Unit = {
    attrPack.configure(paramMap)
  }

  protected def validateNumberOfIncoming(incomingTypes: ConnectionList): Unit = {
    if (incomingTypes.length != inputTypes.length) {
      throw new Exception(s"Error: component inputs number is ${inputTypes.length} while received incoming number is ${incomingTypes.length}")
    }
  }

  /**
    * Validate that the incoming types are compatible with the component definition
    *
    * @param incomingTypes list of incoming types, the order is important
    */
  @throws(classOf[Exception])
  def validateAndPropagateIncomingTypes(incomingTypes: ConnectionList): Unit = {

    validateNumberOfIncoming(incomingTypes)

    // This is a basic full type match check
    for ((incomingType, expectedType) <- incomingTypes zip inputTypes) {
      if (!incomingType.canConnectTo(expectedType)) {
        throw new Exception(s"Error: component $name received type $incomingType while expecting $expectedType")
      }
    }
  }

  /** Generate the DAG portion of the specific component and the specific engine
    *
    * @param envWrapper   Flink environment
    * @param dsArr        Array of DataStream[Any]
    * @param errPrefixStr Error prefix string to use when errors happens during the run of the DAG component
    * @return
    */
  def materialize(envWrapper: EnvWrapperBase, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase]
}
