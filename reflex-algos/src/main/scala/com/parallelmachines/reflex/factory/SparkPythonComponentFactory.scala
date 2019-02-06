package com.parallelmachines.reflex.factory

import com.parallelmachines.reflex.pipeline.ReflexPipelineComponent
import org.slf4j.LoggerFactory

import scala.collection.mutable
import java.nio.file.{Files, Paths}
import java.io.File
import scala.io.Source

/**
  * A factory for Tensorflow python components, each component is provided as a directory containing the component
  * inmplementation (python) and a component.json file which will provide the component signature.
  *
  * @param testMode Register also testing components
  * @param externalDir External directory where to scan for components
  */
class SparkPythonComponentFactory(testMode: Boolean, externalDir:String)
  extends ExternalFileComponentFactory(testMode, externalDir) {


  def apply(compTypeName: String): ReflexPipelineComponent = {
    logger.info(s"SparkPython factory creating: $compTypeName")
    val compInfo = getComponentInfo(compTypeName)
    if (compInfo == null) {
      throw new RuntimeException(s"Trying to fetch a component, which does not exist in spark python " +
        s"component factory! name: $compTypeName")
    }
    val compMeta = parser.parseSignature(compInfo.signature)
    val additionalFiles = detectComponentAdditionalFiles(compInfo)
    if (compMeta.isUserStandalone) {
      new SparkPythonSingleComponent(compMeta, externalDir, additionalFiles)
    } else {
      new SparkPythonComponent(compMeta, externalDir, additionalFiles)
    }
  }
}


object SparkPythonComponentFactory {
  def apply(testMode: Boolean, externalDir: String): SparkPythonComponentFactory =
    new SparkPythonComponentFactory(testMode, externalDir)
}
