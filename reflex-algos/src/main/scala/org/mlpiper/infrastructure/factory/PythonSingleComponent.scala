package org.mlpiper.infrastructure.factory

import org.mlpiper.infrastructure.{ComputeEngineType, ReflexPipelineComponent}

class PythonSingleComponent(componentMetadata: ComponentMetadata, componentDir: String, additionalFiles: List[String])
  extends SingleComponent(componentMetadata, componentDir, additionalFiles) {

  override val engineType = ComputeEngineType.withName(componentMetadata.engineType)
}

object PythonSingleComponent {
  def getAsPythonSingleComponent(comp: ReflexPipelineComponent): PythonSingleComponent = {
    require(comp.engineType == ComputeEngineType.Generic, "Must provide a component of type PythonComponent")
    comp.asInstanceOf[PythonSingleComponent]
  }
}
