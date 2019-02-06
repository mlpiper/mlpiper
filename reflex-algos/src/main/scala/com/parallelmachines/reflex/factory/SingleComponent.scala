package com.parallelmachines.reflex.factory

import java.nio.file.Paths

import com.parallelmachines.reflex.components.flink.streaming.algorithms.ModelBehaviorType
import com.parallelmachines.reflex.pipeline._

import scala.collection.mutable.ArrayBuffer


abstract class SingleComponent(val componentMetadata: ComponentMetadata,
                               val componentsDir: String,
                               val componentAdditionalFiles: List[String])
  extends ReflexPipelineComponent {
  override val isSource: Boolean = true
  override val isUserStandalone: Boolean = componentMetadata.isUserStandalone
  override val isMlStatsUsed: Boolean = componentMetadata.isUsingMLOps
  override val group: String = ComponentsGroups.algorithms
  override val label: String = "Generic Single component pipeline component"
  override val description: String = "Generic Single program component"
  override val version: String = "1.0.0"
  override val inputTypes: ConnectionList = ConnectionList.empty() // zzz
  override var outputTypes: ConnectionList = ConnectionList.empty() // zzz
  var paramMap: Map[String, Any] = _
  var modelOutputPath: Option[String] = None
  var modelInputPath: Option[String] = None
  var modelLogPath: Option[String] = None

  /**
    * Contain the files detected inside the component directory in addition to the main program
    * Such files can later be sent to the running engine.
    */
  override def configure(paramMapArg: Map[String, Any]): Unit = {
    paramMap = paramMapArg
  }

  /**
    * Return the name of the argument which is tagged as model_dir
    *
    * @return
    */
  def modelDirArgument(): Option[String] = componentMetadata.modelDirArgument()

  /**
    * Return the name of the argument tagged as model input path
    * @return
    */
  def modelInputPathArgument(): Option[String] = componentMetadata.inputModelPathArgument()

  /**
    * Return the name of the argument which is tagged as model_version
    *
    * @return
    */
  def modelVersionArgument(): Option[String] = componentMetadata.modelVersionArgument()

  /**
    * Return the name of the argument which is tagged as tflog_dir
    *
    * @return
    */
  def logDirArgument(): Option[String] = componentMetadata.logDirArgument()

  /**
    * Is the component using mlops library - only if this is set to true the system will try to fetch stats from this
    * program
    * @return true if it is supposed to use mlops, false otherwise
    */
  def isUsingMLOps: Boolean = componentMetadata.isUsingMLOps

  /**
    * Is this component/program a model producer
    *
    * @return True if it is a model producer, False otherwise
    */
  def isModelProducer(): Boolean = {
    try {
      val modelBehavior = ModelBehaviorType.withName(componentMetadata.modelBehavior.getOrElse(""))
      ModelBehaviorType.isModelProducer(modelBehavior)
    } catch {
      case _: Throwable => false
    }
  }

  /**
    * Is this component/program a model consumer
    * @return True if it is a model consumer, False otherwise
    */
  def isModelConsumer: Boolean = {
    val modelBehavior = ModelBehaviorType.withName(componentMetadata.modelBehavior.getOrElse(""))
    ModelBehaviorType.isModelConsumer(modelBehavior)
  }

  /**
    * Set the modelOutputPath to the given argument
    *
    * @param outputPath
    */
  def modelOutputPath(outputPath: String): Unit = {
    modelOutputPath = Some(outputPath)
    logger.debug("modelOutputPath set to " + outputPath)
  }

  /**
    * Set the modelInputPath to the given argument
    * @param inputPath
    */
  def modelInputPath(inputPath: String): Unit = {
    modelInputPath = Some(inputPath)
    logger.debug("modelInputPath set to " + inputPath)
  }

  /**
    * Set the ModelLogPath to the given argument
    *
    * @param logPath
    */
  def modelLogPath(logPath: String): Unit = {
    modelLogPath = Some(logPath)
    logger.debug("modelLogPath set to " + logPath)
  }

  /**
    * Get the getLogPath to the given argument
    */
  def getLogPath(): String =  { modelLogPath.toString }

  /**
    * Return the program to use as the main
    *
    * @return
    */
  def programPath(): String = Paths.get(componentsDir, componentMetadata.programPath()).toString


  def componentDir(): String = Paths.get(componentsDir, componentMetadata.name).toString

  /**
    * Building command line from the provided parameters during configure and those provided in the componentMetadata
    * @param overrideModelDir If true then override the model path (input or output) even if it exists in the cmd line
    *                         If false do not override if the model path was provided
    * @return a string containing command line to run this component - --iter 10 --batch_size 5 --output....
    */
  def buildCommandLine(overrideModelDir: Boolean = false): String = {
    var cmdline = ""

    // We should pass only the commandline which are described in the component metadata
    if (paramMap != null) {
      paramMap.foreach { x =>
        var valueToUse = x._2
        val findRes = componentMetadata.arguments.find(_.key == x._1)

        if (findRes.isDefined) {

          if (isModelProducer &&
            modelDirArgument().isDefined &&
            x._1 == modelDirArgument().get &&
            (x._2.toString == "" || overrideModelDir) &&
            modelOutputPath.isDefined) {
            valueToUse = modelOutputPath.get
          }

          if (isModelProducer &&
            logDirArgument().isDefined &&
            x._1 == logDirArgument().get &&
            (x._2.toString == "" || overrideModelDir) &&
            modelLogPath.isDefined) {
            valueToUse = modelLogPath.get
          }

          if (isModelConsumer &&
            modelInputPathArgument().isDefined &&
            x._1 == modelInputPathArgument().get &&
            (x._2.toString == "" || overrideModelDir) &&
            modelInputPath.isDefined) {
            valueToUse = modelInputPath.get
          }

          cmdline += " --" + x._1 + " " + valueToUse.toString
        }
      }

      logger.debug("cmdline after first loop: " + cmdline.trim)
      // The case where the model path argument was not provided by the json
      if (isModelProducer &&
        modelDirArgument().isDefined &&
        !paramMap.contains(modelDirArgument().get) &&
        modelOutputPath.isDefined) {
        logger.debug("Adding modelDirArgument to cmdline")
        cmdline += " --" + modelDirArgument().getOrElse("") + " " + modelOutputPath.get
      }

      if (logDirArgument().isDefined &&
        !paramMap.contains(logDirArgument().get)) {
        logger.debug("Adding logDirArgument to cmdline")
        cmdline += " --" + logDirArgument().getOrElse("") + " " + modelLogPath.get
      }


      // The case where the model path argument was not provided by the json
      if (isModelConsumer &&
        modelInputPathArgument().isDefined &&
        !paramMap.contains(modelInputPathArgument().get) &&
        modelInputPath.isDefined) {
        logger.debug("Adding modelOutputPathArgument to cmdline")
        cmdline += " --" + modelInputPathArgument().get + " " + modelInputPath.get
      }
    }
    logger.info("cmdline: " + cmdline.trim)
    cmdline.trim
  }

  def materialize(envWrapper: EnvWrapperBase, dsArr: ArrayBuffer[DataWrapperBase], errPrefixStr: String):
  ArrayBuffer[DataWrapperBase] = {
    throw new Exception("No materialize for SingleComponent components")
  }
}
