package org.mlpiper.infrastructure.factory

import org.mlpiper.infrastructure.{ComputeEngineType, ReflexPipelineComponent}

class TensorflowComponent(componentMetadata: ComponentMetadata, componentDir: String, additionalFiles: List[String])
  extends SingleComponent(componentMetadata, componentDir, additionalFiles) {

  override val engineType = ComputeEngineType.Tensorflow

}

