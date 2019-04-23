package org.mlpiper.infrastructure.factory

import org.mlpiper.infrastructure
import org.mlpiper.infrastructure._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._


class PythonComponent(componentMetadata: ComponentMetadata, componentDir: String, additionalFiles: List[String])
  extends ReflexPipelineComponent {

  override val engineType: ComputeEngineType.Value = ComputeEngineType.withName(componentMetadata.engineType)
  override val isSource = true
  override val group: String = componentMetadata.group
  override val label: String = componentMetadata.label
  override val description = "Generic Python component"
  override val version = "1.0.0"

  /**
    * Info about the input streams to this components
    *
    * @return
    */
  override val inputTypes: ConnectionList = extractInOutTypes(componentMetadata.inputInfo)

  /**
    * Output streams to this components
    *
    * @return
    */
  override var outputTypes: ConnectionList = extractInOutTypes(componentMetadata.outputInfo)


  def extractInOutTypes(inOutFields: Option[List[InputOutputFields]]): ConnectionList = {

    if (inOutFields.isEmpty) {
      ConnectionList.empty()
    } else {
      val inputFieldsList = inOutFields.get
      if (inputFieldsList.isEmpty) {
        ConnectionList.empty()
      } else {
        val connList = ConnectionList()
        for (inputField <- inputFieldsList) {
          connList.add(
            infrastructure.ComponentConnection(
              tag = typeTag[Any],
              label = inputField.label,
              description = inputField.description,
              group = ConnectionGroups.withName(inputField.group))
          )
        }
        connList
      }
    }
  }

  /** Generate the DAG portion of the specific component and the specific engine
    *
    * @param envWrapper   Environment
    * @param dsArr        Array of DataStream[Any]
    * @param errPrefixStr Error prefix string to use when errors happens during the run of the DAG component
    * @return
    */
  override def materialize(envWrapper: EnvWrapperBase, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String) = {
    throw new Exception("No materialize for Python components")
  }
}

object PythonComponent {
  def getAsPythonComponent(comp: ReflexPipelineComponent): PythonComponent = {
    require(comp.engineType == ComputeEngineType.Generic || comp.engineType == ComputeEngineType.RestModelServing
      || comp.engineType == ComputeEngineType.SageMaker,
      "Must provide a component of type PythonComponent")
    comp.asInstanceOf[PythonComponent]
  }
}

