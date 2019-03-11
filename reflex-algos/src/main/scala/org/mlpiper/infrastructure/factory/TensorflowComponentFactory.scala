package org.mlpiper.infrastructure.factory

import org.mlpiper.infrastructure.ReflexPipelineComponent

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
