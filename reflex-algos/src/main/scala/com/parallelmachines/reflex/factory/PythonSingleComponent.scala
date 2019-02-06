package com.parallelmachines.reflex.factory

import com.parallelmachines.reflex.pipeline._

class PythonSingleComponent(componentMetadata: ComponentMetadata, componentDir: String, additionalFiles: List[String])
  extends SingleComponent(componentMetadata, componentDir, additionalFiles) {

  override val engineType = ComputeEngineType.withName(componentMetadata.engineType)
}

object PythonSingleComponent {
  def getAsPythonSingleComponent(comp: ReflexPipelineComponent): PythonSingleComponent = {
    require(comp.engineType == ComputeEngineType.Python, "Must provide a component of type PythonComponent")
    comp.asInstanceOf[PythonSingleComponent]
  }
}
