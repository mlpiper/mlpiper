package com.parallelmachines.reflex.factory

import com.parallelmachines.reflex.pipeline._

class TensorflowComponent(componentMetadata: ComponentMetadata, componentDir: String, additionalFiles: List[String])
  extends SingleComponent(componentMetadata, componentDir, additionalFiles) {

  override val engineType = ComputeEngineType.Tensorflow

  /**
    * If the user provides a value, we do not override it.
    * To do this, the json must provide a parameter that
    * is tagged as the modelDirArgument.
    * If the tagged argument is provided but left blank,
    * then we override it with the system
    * parameter modelOutputPath.
    * This function is called from java, so it cannot return
    * Options.
    * @return String representing model save directory or empty string
    */
  def getModelSaveDirectory(): String = {
    if (logDirArgument.isDefined) {
      val modelDirArg = modelDirArgument.get
      val modelDir = paramMap.get(modelDirArg)

      modelDir match {
        case Some(s) => {
          val dir = s.asInstanceOf[String]
          if (dir.length() > 0) {
            return s.asInstanceOf[String]
          }
        }
        case None => {}
      }
    }

    if (modelOutputPath.isDefined) {
      logger.debug("getModelSaveDirectory: using modelOutputPath")
      return modelOutputPath.get
    }

    logger.debug("getModelSaveDirectory: found nothing")

    return ""
  }

  /**
    * To set this, the user must provide a value in a parameter
    * tagged as the modelVersionArgument. Otherwise, it is left blank.
    * This function is called from java, so it cannot return
    * Options.
    * @return String representing model version or empty string
    */
  def getModelVersion(): String = {
    if (modelVersionArgument.isDefined) {
      val modelVersionArg = modelVersionArgument.get
      val modelVersion = paramMap.get(modelVersionArg)

      modelVersion match {
        case Some(s) => {
          val vers = s.asInstanceOf[String]
          if (vers.length() > 0) {
            return vers
          }
        }
        case None => {}
      }
    }
    logger.debug("getModelVersion: modelVersion return empty string")

    return ""
  }

  /**
    * If the user provides a value, we do not override it.
    * To do this, the json must provide a parameter that
    * is tagged as the logDirArgument.
    * If the tagged argument is provided but left blank,
    * then we override it with the system
    * parameter modelLogPath.
    * This function is called from java, so it cannot return
    * Options.
    * @return String representing log directory or empty string
    */
  def getLogSaveDirectory(): String = {
    if (logDirArgument.isDefined) {
      val logDirArg = logDirArgument.get
      val logDir = paramMap.get(logDirArg)

      logDir match {
        case Some(s) => {
          val dir = s.asInstanceOf[String]
          if (dir.length() > 0) {
            return s.asInstanceOf[String]
          }
        }
        case None => {}
      }
    }

    if (modelLogPath.isDefined) {
      logger.debug("getLogSaveDirectory: using modelLogPath")
      return modelLogPath.get
    }

    logger.debug("getLogSaveDirectory: found nothing")

    return ""
  }
}


object TensorflowComponent {
  def getAsTensorflowComponent(comp: ReflexPipelineComponent): TensorflowComponent = {
    require(comp.engineType == ComputeEngineType.Tensorflow, "Must provide a component of type Tensorflow")
    comp.asInstanceOf[TensorflowComponent]
  }
}
