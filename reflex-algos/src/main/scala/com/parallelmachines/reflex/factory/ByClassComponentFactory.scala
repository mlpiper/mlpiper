package com.parallelmachines.reflex.factory

import com.parallelmachines.reflex.factory.ReflexComponentFactory.{engines, getEngineFactory}
import com.parallelmachines.reflex.pipeline.{ComputeEngineType, ReflexPipelineComponent}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * A base class for component factory based on class names (code incorporated in the sources of Reflex)
  * @param engineType Engine type to set
  * @param classList List of class names to register
  */
class ByClassComponentFactory(val engineType: ComputeEngineType.Value,
                              val classList: mutable.MutableList[Class[_]]) extends EngineComponentFactory {
  protected val logger = LoggerFactory.getLogger(getClass)
  registerAllComponents()

  private def registerAllComponents(): Unit = {
    for (compClass <- classList) {
      registerComponent(compClass)
    }
  }

  def unRegisterComponents() : Unit = {
    components.clear()
  }

  override def reconfigure(): Unit = {
    logger.info("Running reconfigure for byclass factory - doing nothing")
  }

  /**
    * Provide the ability to register a component directly - used for testing
    * @param compClass The class to register
    */
  def registerComponent(compClass: Class[_]): Unit = {
    val comp = Class.forName(compClass.getCanonicalName).newInstance().asInstanceOf[ReflexPipelineComponent]
    if (comp.engineType != engineType) {
      throw new Exception(s"Trying to register component: '${compClass.getCanonicalName}' with engineType: '${comp.engineType}' to the engine '${engineType}'")
    }

    // TODO: let the validate throw the exception.
    try {
      comp.inputTypes.validate()
      comp.outputTypes.validate()
    } catch {
      case e: Exception =>
        throw new Exception(s"Found ConnectionList with the duplicate label in the component: ${compClass.toString}. ${e.getMessage}")
    }

    for (compInfo <- components) {
      if ((compInfo.canonicalName == compClass.getCanonicalName) ||
        (compInfo.typeName == compClass.getSimpleName)) {
        throw new Exception(s"Component '${compClass.getCanonicalName}' already registered for '${engineType}' engine. Registered component: '${compInfo.canonicalName}'")
      }
    }
    components += ComponentInfo(compClass.getPackage.getName, compClass.getCanonicalName, compClass.getSimpleName,
      classAvail = true, comp.getInfo, componentMetadata = None)
  }

  override def getComponentInfo(compTypeName: String): ComponentInfo = {
    for (compInfo <- components) {
      if (compInfo.typeName == compTypeName ||
        compInfo.canonicalName == compTypeName) {
          return compInfo
        }
      }
    null
  }

  def componentSignature(typeName: String): String = {
    val compInfo = getComponentInfo(typeName)
    compInfo.signature
  }

  def apply(compTypeName: String): ReflexPipelineComponent = {

    val compInfo = getComponentInfo(compTypeName)
    if (compInfo != null) {
      val comp = Class.forName(compInfo.canonicalName).newInstance().asInstanceOf[ReflexPipelineComponent]
      return comp
    }

    throw new Exception(s"Error: component '$compTypeName' is not supported by '$engineType' engine")
  }

}