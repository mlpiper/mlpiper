package org.mlpiper.infrastructure.factory

import org.mlpiper.infrastructure.{ComputeEngineType, ReflexPipelineComponent}

/**
  * A factory for Python components, each component is provided as a directory containing the component
  * inmplementation (python) and a component.json file which will provide the component signature.
  *
  * @param testMode Register also testing components
  * @param externalDir External directory where to scan for components
  */
class PythonComponentFactory(testMode: Boolean, externalDir:String)
  extends ExternalFileComponentFactory(testMode, externalDir) {


  def apply(compTypeName: String): ReflexPipelineComponent = {
    logger.debug(s"Python factory creating: $compTypeName")
    val compInfo = getComponentInfo(compTypeName)
    if (compInfo == null) {
      throw new RuntimeException(s"Trying to fetch a component, which does not exist in python " +
        s"component factory! name: $compTypeName")
    }
    val compMeta = parser.parseSignature(compInfo.signature)
    val additionalFiles = detectComponentAdditionalFiles(compInfo)
    if (compMeta.isUserStandalone &&
        ComputeEngineType.withName(compMeta.engineType) != ComputeEngineType.RestModelServing) {
      new PythonSingleComponent(compMeta, externalDir, additionalFiles)
    } else {
      new PythonComponent(compMeta, externalDir, additionalFiles)
    }
  }
}


object PythonComponentFactory {
  def apply(testMode: Boolean, externalDir: String): PythonComponentFactory =
    new PythonComponentFactory(testMode, externalDir)
}

