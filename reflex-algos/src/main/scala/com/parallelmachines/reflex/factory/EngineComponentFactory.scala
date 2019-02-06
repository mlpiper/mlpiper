package com.parallelmachines.reflex.factory


import com.parallelmachines.reflex.pipeline.{ComputeEngineType, ReflexPipelineComponent}

import scala.collection.mutable
import scala.collection.JavaConverters._

trait EngineComponentFactory {

  var components: mutable.MutableList[ComponentInfo] = mutable.MutableList[ComponentInfo]()

  def getComponentsJava : java.util.List[ComponentInfo] = {
    components.asJava
  }

  /**
    * Provide a way to release resources/unregister
    */
  def unRegisterComponents() : Unit


  /**
    * Provide a way to reconfigure the engine - for example for reloading data from file-system
    */
  def reconfigure(): Unit

  /**
    * Return the component info for the compTypeName
    * @param compTypeName The name of the component to look for
    * @return CompInfo object containing the info about the component
    */
  def getComponentInfo(compTypeName: String): ComponentInfo

  /**
    * Return the component JSON signature
    * @param typeName Component name to search for
    * @return JSON string containing the component signature
    */
  def componentSignature(typeName: String): String

  /**
    * Create the requested component and return it
    * @param compTypeName The component name to create
    * @return
    */
  def apply(compTypeName: String): ReflexPipelineComponent
}
