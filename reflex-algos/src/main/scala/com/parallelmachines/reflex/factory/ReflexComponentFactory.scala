package com.parallelmachines.reflex.factory

import java.nio.file.Paths

import com.parallelmachines.reflex.components.flink.batch.FlinkBatchComponentFactory
import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponentFactory
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponentFactory
import com.parallelmachines.reflex.pipeline.DagGen._
import com.parallelmachines.reflex.pipeline._
import org.slf4j.LoggerFactory
import java.io.File

import org.apache.commons.io.FileUtils

import scala.collection.mutable
import scala.util.parsing.json.{JSON, JSONArray, JSONFormat, JSONObject}
import scala.reflect.runtime.universe._


object ReflexComponentFactory {
  var engines: mutable.Map[ComputeEngineType.Value, EngineComponentFactory] = mutable.Map[ComputeEngineType.Value, EngineComponentFactory]()
  protected val logger = LoggerFactory.getLogger(getClass)

  private var externalComponentsDir: String = ""
  protected var initDone: Boolean = false

  /**
    * This method is for java code to be able to set this variable - object variables are private from java
    * @param dir
    */
  def setExternalComponentsDir(dir: String): Unit = {
    externalComponentsDir = dir
  }

  private def checkExternalComponensDir(): Unit = {
    val f = new File(externalComponentsDir)
    if (!f.exists || !f.isDirectory) {
      throw new Exception(s"External components directory [$externalComponentsDir] does not exist or not a directory")
    }
  }
  def init(): Unit = {
    this.synchronized {
      if (!initDone) {
        unRegisterEngines()
      } else {
        logger.warn("Init for component factory was already called - ignoring")
      }
    }
  }

  def cleanup(): Unit = {
    this.synchronized {
      unRegisterEngines()
    }
  }

  def isEngineRegistered(engineType: ComputeEngineType.Value) : Boolean = {
    return engines.contains(engineType)
  }

  def registerEngineFactory(engineType: ComputeEngineType.Value, engineFactory: EngineComponentFactory): Unit= {
    this.synchronized {
      if (!engines.contains(engineType)) {
        engines(engineType) = engineFactory
      }
    }
  }

  def reconfigureEngineFactory(engineType: ComputeEngineType.Value): Unit = {
    this.synchronized {
      logger.info(s"Reconfiguring engine $engineType")
      val engineFactory = getEngineFactory(engineType)
      engineFactory.reconfigure()
    }
  }

  def registerEngineFactory(engineType: ComputeEngineType.Value, testMode: Boolean): Unit = {

    // TODO: instead of masking a potential error we might want to let the
    // caller to check if the engine is registered and decide what to do
    if (isEngineRegistered(engineType)) {
      return
    }

    checkExternalComponensDir()

    engineType match {
      case ComputeEngineType.FlinkBatch =>
        registerEngineFactory(ComputeEngineType.FlinkBatch, FlinkBatchComponentFactory(testMode))
      case ComputeEngineType.FlinkStreaming =>
        registerEngineFactory(ComputeEngineType.FlinkStreaming, FlinkStreamingComponentFactory(testMode))
      case ComputeEngineType.SparkBatch =>
        registerEngineFactory(ComputeEngineType.SparkBatch, SparkBatchComponentFactory(testMode))
      case ComputeEngineType.PySpark => {
        val sparkPythonDir = Paths.get(externalComponentsDir, ComputeEngineType.PySpark.toString).toString
        ReflexComponentFactory.registerEngineFactory(
          ComputeEngineType.PySpark,
          SparkPythonComponentFactory(testMode, sparkPythonDir))
      }
      case ComputeEngineType.Tensorflow => {
        val tfDir = Paths.get(externalComponentsDir, ComputeEngineType.Tensorflow.toString).toString
        ReflexComponentFactory.registerEngineFactory(
          ComputeEngineType.Tensorflow,
          TensorflowComponentFactory(testMode, tfDir))
      }
      case ComputeEngineType.Python => {
        val pythonDir = Paths.get(externalComponentsDir, ComputeEngineType.Python.toString).toString
        ReflexComponentFactory.registerEngineFactory(
          ComputeEngineType.Python,
          SparkPythonComponentFactory(testMode, pythonDir))
      }
      case ComputeEngineType.RestModelServing => {
        val restModelServingDir = Paths.get(externalComponentsDir, ComputeEngineType.RestModelServing.toString).toString
        ReflexComponentFactory.registerEngineFactory(
          ComputeEngineType.RestModelServing,
          SparkPythonComponentFactory(testMode, restModelServingDir))
      }
    }
  }

  def registerAllEngines(externalComponentsDir: String): Unit = {
    logger.info("Registering all engines, externalComponentsDir: " + externalComponentsDir)

    // TODO: the unregister is added here to make sure the registerAllEngines call is always working.
    // TODO: this is a fix for an issue where registerAllEngines is called multiple times.
    unRegisterEngines()
    val testMode = false
    registerEngineFactory(ComputeEngineType.FlinkBatch, FlinkBatchComponentFactory(testMode))
    registerEngineFactory(ComputeEngineType.FlinkStreaming, FlinkStreamingComponentFactory(testMode))
    registerEngineFactory(ComputeEngineType.SparkBatch, SparkBatchComponentFactory(testMode))

    val tfDir = Paths.get(externalComponentsDir, ComputeEngineType.Tensorflow.toString).toString
    ReflexComponentFactory.registerEngineFactory(
      ComputeEngineType.Tensorflow,
      TensorflowComponentFactory(testMode, tfDir))

    val sparkPythonDir = Paths.get(externalComponentsDir, ComputeEngineType.PySpark.toString).toString
    ReflexComponentFactory.registerEngineFactory(
      ComputeEngineType.PySpark,
      SparkPythonComponentFactory(testMode, sparkPythonDir))

    val pythonDir = Paths.get(externalComponentsDir, ComputeEngineType.Python.toString).toString
    ReflexComponentFactory.registerEngineFactory(
      ComputeEngineType.Python,
      PythonComponentFactory(testMode, pythonDir))

    val restModelServingDir = Paths.get(externalComponentsDir, ComputeEngineType.RestModelServing.toString).toString
    ReflexComponentFactory.registerEngineFactory(
      ComputeEngineType.RestModelServing,
      PythonComponentFactory(testMode, restModelServingDir))

  }

  private def unRegisterEngines(): Unit = {
    for ((_, engineInfo) <- engines) {
      engineInfo.unRegisterComponents()
    }
    engines.clear()
  }

  def getEngineFactory(engineType: ComputeEngineType.Value): EngineComponentFactory = {
    require(engines.contains(engineType), s"Not supported engine: $engineType")
    engines(engineType)
  }

  def getEngineFactoryOfExternalFile(engineType: ComputeEngineType.Value): ExternalFileComponentFactory = {
    require(engines.contains(engineType), s"Not supported engine: $engineType")
    engines(engineType).asInstanceOf[ExternalFileComponentFactory]
  }

  def apply(engineType: ComputeEngineType.Value, compTypeName: String, argMap: Map[String, Any] = null): ReflexPipelineComponent = {
    require(engines.contains(engineType), s"Not supported engine: $engineType")
    val comp = getEngineFactory(engineType).apply(compTypeName)
    if (argMap != null) {
      comp.configure(argMap)
    }
    comp
  }

  def componentSignature(engineType: ComputeEngineType.Value, compTypeName: String): String = {
    require(engines.contains(engineType), s"Not supported engine: $engineType")
    getEngineFactory(engineType).componentSignature(compTypeName)
  }

  def componentInfo(engineType: ComputeEngineType.Value, compTypeName: String): ComponentInfo = {
    require(engines.contains(engineType), s"Not supported engine: $engineType")
    getEngineFactory(engineType).getComponentInfo(compTypeName)
  }

  def registerComponentFromDir(compDir: String, metadataFilename: String, update: Boolean): ComponentMetadata = {
    logger.info(s"Registering new component directory $compDir")

    checkExternalComponensDir()

    val compMeta = ExternalDirComponentUtil.verifyComponentDir(compDir, metadataFilename)

    if (!ExternalDirEngines.isExternalDirEngine(compMeta.engineType)) {
      throw new Exception(s"Engine type ${compMeta.engineType} can not be registered from a directory")
    }

    // Register the engine repo
    registerEngineFactory(ComputeEngineType.withName(compMeta.engineType), testMode = false)

    // TODO: we should consider using git to manage the component file repository

    val destDir = Paths.get(externalComponentsDir, compMeta.engineType, compMeta.name).toString
    logger.info(s"Destination path is: $destDir")

    val compDirFile = new File(compDir)
    val destDirFile = new File(destDir)

    val engineFactory = getEngineFactoryOfExternalFile(ComputeEngineType.withName(compMeta.engineType));
    if (destDirFile.exists()) {
      engineFactory.unRegisterComponent(compMeta.name)
      FileUtils.deleteDirectory(destDirFile);
    }

    // TODO: make sure the rename was ok
    compDirFile.renameTo(destDirFile)

    engineFactory.registerCompDir(destDirFile)
    compMeta
  }

  def unregisterComponentDir(engineType: ComputeEngineType.Value, compTypeName: String): Unit = {
    logger.info(s"Un-registering component directory, engineType: $engineType, name: $compTypeName")

    checkExternalComponensDir()

    val internalCompDir = Paths.get(externalComponentsDir, engineType.toString, compTypeName).toString

    val internalCompDirFile = new File(internalCompDir)
    if (!internalCompDirFile.exists()) {
      throw new Exception(s"Failed to unregister component! Component's dir not found! dir: $internalCompDir")
    }

    val engineFactory = getEngineFactoryOfExternalFile(engineType)
    engineFactory.unRegisterComponent(compTypeName)

    logger.info(s"Removing component dir: $internalCompDir")
    FileUtils.deleteDirectory(internalCompDirFile)
  }

  private def formatJSON(json: Any, indentLevel: Int = 2): String = json match {
    case o: JSONObject =>
      o.obj.map { case (k, v) =>
        "  " * (indentLevel + 1) + JSONFormat.defaultFormatter(k) + ": " + formatJSON(v, indentLevel + 1)
      }.mkString("{\n", ",\n", "\n" + "  " * indentLevel + "}")

    case a: JSONArray =>
      a.list.map {
        e => "  " * (indentLevel + 1) + formatJSON(e, indentLevel + 1)
      }.mkString("[\n", ",\n", "\n" + "  " * indentLevel + "]")

    case _ =>
      JSONFormat defaultFormatter json
  }

  private def systemConfigJSONStr: String = {
    val fieldList = typeOf[ReflexSystemConfig].members.filter(!_.isMethod).map(_.name).map(x => "\"" + x.toString.trim + "\"")
    "[" + fieldList.mkString(",") + "]"
  }

  /**
    * Generate a JSON string describing the Reflex components defined
    *
    * @return String containing the JSON description of all components
    */
  def generateComponentsDescriptionJSON(): String = {
    var ManifestJSON: String = ""
    for ((engineType, engineInfo) <- engines) {
      for (compInfo <- engineInfo.components) {
        if (!compInfo.isTransient()) {
          val signature = componentSignature(engineType, compInfo.typeName)
          ManifestJSON += (s"""$signature""" + ",")
        } else {
          logger.info(s"Skipping transient component signature description! name: ${compInfo.canonicalName}")
        }
      }
    }

    ManifestJSON = ManifestJSON.dropRight(1)
    ManifestJSON =
      s"""{
                "${JsonHeaders.JsonVersionHeader}":"$jsonVersion",
                "${JsonHeaders.ManifestHeader}":"Reflex algorithms",
                "${JsonHeaders.VersionHeader}":"$dagVersion",
                "${JsonHeaders.SystemConfigHeader}": $systemConfigJSONStr,
                "${JsonHeaders.ComponentsHeader}":[$ManifestJSON]
                }""".stripMargin
    val res = JSON.parseRaw(ManifestJSON)
    if (res.isEmpty) {
      throw new Exception(s"Error parsing components info JSON - [$ManifestJSON]")
    }
    formatJSON(res.get)
  }

  def generateBuiltinComponentsDescriptionJSON(): String = {
    var ManifestJSON: String = ""
    val builtinEngines = engines.filterNot(p => p._2.isInstanceOf[ExternalFileComponentFactory])

    val builtinEnginesNum = builtinEngines.size
    logger.info(s"Generating builtin comps $builtinEnginesNum")
    for ((engineType, engineInfo) <- builtinEngines) {
      for (compInfo <- engineInfo.components) {
        if (!compInfo.isTransient()) {
          val signature = componentSignature(engineType, compInfo.typeName)
          ManifestJSON += (s"""$signature""" + ",")
        } else {
          logger.info(s"Skipping transient component signature description! name: ${compInfo.canonicalName}")
        }
      }
    }
    ManifestJSON = ManifestJSON.dropRight(1)
    ManifestJSON =
      s"""{
                "${JsonHeaders.JsonVersionHeader}":"$jsonVersion",
                "${JsonHeaders.ManifestHeader}":"Reflex algorithms",
                "${JsonHeaders.VersionHeader}":"$dagVersion",
                "${JsonHeaders.SystemConfigHeader}": $systemConfigJSONStr,
                "${JsonHeaders.ComponentsHeader}":[$ManifestJSON]
                }""".stripMargin
    val res = JSON.parseRaw(ManifestJSON)
    if (res.isEmpty) {
      throw new Exception(s"Error parsing components info JSON - [$ManifestJSON]")
    }
    formatJSON(res.get)
  }

}

