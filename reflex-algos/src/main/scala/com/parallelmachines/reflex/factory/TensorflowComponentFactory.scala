package com.parallelmachines.reflex.factory

import com.parallelmachines.reflex.pipeline.ReflexPipelineComponent
import org.slf4j.LoggerFactory

import scala.collection.mutable
import java.nio.file.{Files, Paths}
import java.io.File
import scala.io.Source

class TensorflowComponentFactory(testMode: Boolean, externalDir: String)
  extends ExternalFileComponentFactory(testMode, externalDir) {

  def apply(compTypeName: String): ReflexPipelineComponent = {

    logger.info(s"Tensorflow factory creating: $compTypeName")
    val compInfo = getComponentInfo(compTypeName)
    if (compInfo == null) {
      throw new RuntimeException(s"Trying to fetch a component, which does not exist in tensorflow " +
        s"component factory! name: $compTypeName")
    }
    val compMeta = parser.parseSignature(compInfo.signature)
    val additionalFiles = detectComponentAdditionalFiles(compInfo)
    new TensorflowComponent(compInfo.componentMetadata.get, externalDir, additionalFiles)
  }
}

object TensorflowComponentFactory {
  def apply(testMode: Boolean, externalDir: String): TensorflowComponentFactory =
    new TensorflowComponentFactory(testMode, externalDir)
}
