package com.parallelmachines.reflex.test.reflexpipeline

import java.nio.file.{Files, Path, Paths}

import com.parallelmachines.reflex.components.flink.streaming.FlinkStreamingComponentFactory
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponentFactory
import org.mlpiper.infrastructure.factory.{PythonComponentFactory,ReflexComponentFactory}
import org.mlpiper.infrastructure.{ComputeEngineType, ReflexPipelineBuilder, ReflexPipelineDag}

/**
  * Helper object for tests to run the parsing + validate in one run
  */
object DagTestUtil {

  private val componentsDir = "components"

  def parseGenerateValidate(jsonStr: String): ReflexPipelineDag = {
    new ReflexPipelineBuilder().buildPipelineFromJson(jsonStr)
  }

  def initComponentFactory(): Unit = {
    ReflexComponentFactory.init()
    ReflexComponentFactory.registerEngineFactory(ComputeEngineType.FlinkStreaming, FlinkStreamingComponentFactory(true))
    ReflexComponentFactory.registerEngineFactory(ComputeEngineType.SparkBatch, SparkBatchComponentFactory(true))

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val tfPath = resolveBaseDirPath(currentDirectory, componentsDir, ComputeEngineType.Tensorflow.toString)

    ReflexComponentFactory.registerEngineFactory(ComputeEngineType.Tensorflow, PythonComponentFactory(testMode = true, tfPath.toString))
  }

  def getComponentsDir: String = {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val compDirPath = resolveBaseDirPath(currentDirectory, componentsDir)
    compDirPath.toString
  }

  /*
    TODO: find more elegant solution or decide on components folder location
    - when running from Intellij working dir will be ~/reflex-algos
    - when running from Maven working dir is ~/reflex-algos/target
    but components/Tensorflow is located in ~/reflex-algos
  */

  def resolveBaseDirPath(first: String, more: String*): Path = {
    var basePath = Paths.get(first, more.toArray: _*)
    if (Files.notExists(basePath)) {
      basePath = Paths.get(first.replace("target", ""), more.toArray: _*)
    }
    if (Files.notExists(basePath)) {
      throw new Exception(s"Can not resolve Components path: `${basePath.toString}`")
    }
    basePath
  }
}
